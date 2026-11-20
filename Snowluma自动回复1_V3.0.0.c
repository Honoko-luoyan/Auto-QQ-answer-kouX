/*
 * Snowluma 群消息自动回复工具（C 语言版）—— 多端口版本 V3.0.0
 *
 * V3.0.0 功能总览：
 *   1. 连接层：同时监听 3001/3002/3003/3004 四个正向 WebSocket 端口；
 *      启动时扫描可用性并打印，不可用端口仅提示、不重连；
 *      可用端口断线自动重连（3 秒间隔）。
 *   2. 触发规则：仅监听群内指定成员（TARGET_QQ，其他人完全静默忽略），
 *      其消息含「扣X」时立即回复 X 本身——X 为「扣」后第一个任意字
 *      （数字 / 汉字均可，UTF-8 感知，自动跳过空格；例：扣1->1，扣贰->贰，扣可->可）。
 *   3. 回复确认：send_group_msg 带 echo，解析 OneBot 响应，
 *      只有 Snowluma 确认成功才进入 1 分钟冷却（每端口独立）。
 *   4. 禁言处理（防刷屏设计）：
 *      - 回复被拒（如被禁言）后不再做任何轮询，进入「等待解禁通知」状态；
 *      - 收到 Snowluma 的 group_ban 解禁通知（本账号或全员）立即回复一次；
 *      - 超过 2 分钟仍未收到解禁通知则放弃本次回复（不进入冷却）；
 *      - 响应丢失绝不盲目补发；每次触发发送次数硬上限 20 条。
 *   5. 控制台：WriteConsoleW 输出中文（任何代码页不乱码）；
 *      ReadConsoleW 读取输入；输入码页 65001 抑制输入法状态条。
 * 回复速度：正常路径 <1ms（TCP_NODELAY + 事件即时回复）。
 * 防刷屏：任何单次触发对群内产生的消息最多 20 条，成功即停。
 */

#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#define HOST                "127.0.0.1"
#define TARGET_QQ           "3288574505" /* 仅监听该成员的消息，其他人静默忽略 */
#define PORT_COUNT          4
#define RECV_BUF            65536
#define REPLY_COOLDOWN      60          /* 触发后冷却秒数 */
#define RECONNECT_DELAY     3           /* 断线重连间隔（秒） */
#define CONNECT_TIMEOUT_MS  800         /* 建立连接超时 */
#define PROBE_TIMEOUT_MS    400         /* 启动扫描端口超时 */
#define SELECT_TIMEOUT_MS   300         /* 主循环 select 间隔 */
#define MUTE_POLL_SECONDS      120  /* 触发后发现被禁言：等待解禁通知的时长（秒） */
#define MAX_SEND_ATTEMPTS      20   /* 每次触发的发送次数硬上限（防刷屏） */

static const int g_ports[PORT_COUNT] = { 3001, 3002, 3003, 3004 };
typedef struct {
    int                 port;           /* 端口号 */
    SOCKET              sock;           /* 套接字，未连接时为 INVALID_SOCKET */
    int                 available;      /* 启动扫描结果：1 可用 / 0 不可用 */
    int                 connected;      /* 当前是否已连接 */
    unsigned long long  next_try;       /* 下次重连时刻（GetTickCount64） */
    time_t              cooldown_until; /* 本端口（本账号）的冷却截止时间 */
    int                 pending_reply;  /* 回复流程进行中（等待 OneBot 结果） */
    int                 awaiting;       /* 最新一次发送的响应尚未到达 */
    char                reply_echo[32]; /* 当前待匹配的 echo */
    char                trig_user[64];  /* 触发本次回复的群成员 */
    char                trig_reply[16]; /* 本次回复内容（「扣」后的字 X） */
    int                 mute_poll;      /* 禁言轮询模式 */
    unsigned long long  poll_start;     /* 进入轮询的时刻 */
    unsigned long long  mute_poll_until;/* 轮询截止时刻 */
    unsigned long long  next_poll;      /* 下次重试时刻 */
    unsigned long long  last_send_tick; /* 最近一次发送回复的时刻 */
    char                self_id[32];    /* 机器人自身 QQ（来自事件 self_id） */
    int                 send_attempts;  /* 本次触发已发送次数（硬上限防刷屏） */
    char                buf[RECV_BUF];  /* 接收缓冲 */
    size_t              len;            /* 缓冲中已收字节数 */
} Conn;

static Conn         g_conns[PORT_COUNT];
static long long    g_group_id = 0;
static volatile int g_running = 1;
static unsigned long g_echo_seq = 0;        /* 回复 echo 自增序号 */

/* ------------------------------------------------------------------ */
/* 输出                                                                */
/*                                                                     */
/* 不把控制台代码页强制改成 65001（UTF-8）：中文 Windows 的控制台字体   */
/* 是 GBK 字库，UTF-8 字节流在 65001 下取不到字形，中文会全变成 ◆。     */
/* 这里优先用 WriteConsoleW 直接把 UTF-16 交给控制台，由系统选字形，     */
/* 任何代码页 / 字体下都能正常显示；万一 WriteConsoleW 不可用，则退回    */
/* 按控制台当前代码页（如 GBK）转码输出；输出被重定向时写 UTF-8 字节。   */
/* ------------------------------------------------------------------ */

static HANDLE g_out = INVALID_HANDLE_VALUE;
static int    g_out_is_console = 0;

static void out_init(void)
{
    DWORD mode;

    g_out = GetStdHandle(STD_OUTPUT_HANDLE);
    if (g_out != INVALID_HANDLE_VALUE && g_out != NULL
        && GetConsoleMode(g_out, &mode))
        g_out_is_console = 1;
}

static void out_text(const char *utf8)
{
    if (g_out_is_console) {
        wchar_t wbuf[4096];
        int     n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, wbuf,
                                        (int)(sizeof wbuf / sizeof wbuf[0]));
        if (n > 1) {
            DWORD written = 0;

            if (WriteConsoleW(g_out, wbuf, (DWORD)(n - 1), &written, NULL))
                return;

            /* 兜底：按控制台当前代码页转码后写入 */
            {
                char mb[8192];
                UINT cp = GetConsoleOutputCP();
                int  m;

                if (cp == 0 || cp == CP_UTF8)
                    cp = CP_UTF8;
                m = WideCharToMultiByte(cp, 0, wbuf, n - 1, mb,
                                        (int)sizeof mb - 1, NULL, NULL);
                if (m > 0) {
                    mb[m] = '\0';
                    fwrite(mb, 1, (size_t)m, stdout);
                    fflush(stdout);
                    return;
                }
            }
        }
    }
    fputs(utf8, stdout);
    fflush(stdout);
}

static void out_fmt(const char *fmt, ...)
{
    char    buf[4096];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    out_text(buf);
}

/* ------------------------------------------------------------------ */
/* 控制台输入                                                          */
/*                                                                     */
/* 键盘“没反应”的常见原因都在控制台这一层，这里逐一处理：              */
/*   1. 窗口没拿到焦点        -> 提示前把窗口置前                      */
/*   2. 鼠标点过窗口进入标记/选择状态（快速编辑模式）                  */
/*                            -> 读输入期间临时关掉快速编辑             */
/*   3. 日志翻页后视口不在底部-> 输入前把窗口滚到最底部                */
/*   4. 中文输入法全角数字    -> 读入后统一转成半角                    */
/* 输入用 ReadConsoleW 直接拿 UTF-16，不受控制台代码页影响；标准输入被 */
/* 重定向（管道/文件）时仍走 fgets。                                    */
/* ------------------------------------------------------------------ */

static HANDLE g_in = INVALID_HANDLE_VALUE;
static int    g_in_is_console = 0;
static DWORD  g_in_mode_saved = 0;
static int    g_in_mode_ok = 0;

static void in_init(void)
{
    DWORD mode;

    /* 输入代码页设为 65001：控制台处于该代码页时 conhost 会禁用输入法，
       左下角不再出现「英文 / 拼音 / 半」状态条，按键也不会被输入法吃掉。
       只改输入侧，输出仍走 WriteConsoleW，不受代码页影响，中文照常显示。 */
    SetConsoleCP(65001);

    g_in = GetStdHandle(STD_INPUT_HANDLE);
    if (g_in != INVALID_HANDLE_VALUE && g_in != NULL && GetConsoleMode(g_in, &mode))
        g_in_is_console = 1;
}

static void in_prepare(void)
{
    HWND  hwnd;
    DWORD mode;

    if (!g_in_is_console)
        return;

    hwnd = GetConsoleWindow();
    if (hwnd != NULL) {
        if (IsIconic(hwnd))
            ShowWindow(hwnd, SW_RESTORE);
        SetForegroundWindow(hwnd);
        BringWindowToTop(hwnd);
    }

    if (GetConsoleMode(g_in, &mode)) {
        g_in_mode_saved = mode;
        g_in_mode_ok    = 1;
        mode |= ENABLE_EXTENDED_FLAGS;
        mode &= ~ENABLE_QUICK_EDIT_MODE;
        SetConsoleMode(g_in, mode);
    }

    /* 日志翻页后把视口拉回底部，否则输入的回显在可视区外，看着像没反应 */
    {
        CONSOLE_SCREEN_BUFFER_INFO csbi;

        if (GetConsoleScreenBufferInfo(g_out, &csbi)
            && csbi.dwCursorPosition.Y >= csbi.srWindow.Bottom) {
            SMALL_RECT r;
            SHORT      height = (SHORT)(csbi.srWindow.Bottom - csbi.srWindow.Top + 1);
            SHORT      top    = (SHORT)(csbi.dwCursorPosition.Y - height + 1);

            if (top < 0)
                top = 0;
            r.Left   = 0;
            r.Top    = top;
            r.Right  = (SHORT)(csbi.dwSize.X - 1);
            r.Bottom = csbi.dwCursorPosition.Y;
            SetConsoleWindowInfo(g_out, TRUE, &r);
        }
    }
}

static void in_restore(void)
{
    if (g_in_is_console && g_in_mode_ok) {
        SetConsoleMode(g_in, g_in_mode_saved);
        g_in_mode_ok = 0;
    }
}

/* 读一行；成功返回 1，EOF/失败返回 0 */
static int in_read_line(char *out, size_t outsz)
{
    if (outsz == 0)
        return 0;
    out[0] = '\0';

    if (g_in_is_console) {
        wchar_t w[512];
        DWORD   n = 0;
        size_t  i;

        if (!ReadConsoleW(g_in, w, (DWORD)(sizeof w / sizeof w[0]) - 1, &n, NULL))
            return 0;
        w[n] = L'\0';
        while (n > 0 && (w[n - 1] == L'\r' || w[n - 1] == L'\n'))
            w[--n] = L'\0';

        for (i = 0; i < n; i++) {
            if (w[i] >= 0xFF10 && w[i] <= 0xFF19)       /* 全角 ０-９ */
                w[i] = (wchar_t)(L'0' + (w[i] - 0xFF10));
            else if (w[i] == 0x3000)                    /* 全角空格 */
                w[i] = L' ';
        }

        if (WideCharToMultiByte(CP_UTF8, 0, w, (int)n + 1, out, (int)outsz,
                                NULL, NULL) <= 0)
            return 0;
        return 1;
    }

    if (fgets(out, (int)outsz, stdin) == NULL)
        return 0;
    out_text("\n");
    return 1;
}

/* ------------------------------------------------------------------ */
/* 日志                                                                */
/* ------------------------------------------------------------------ */

static void log_stamp(void)
{
    time_t    now = time(NULL);
    struct tm tmv;
    char      ts[16];

    localtime_s(&tmv, &now);
    strftime(ts, sizeof ts, "%H:%M:%S", &tmv);
    out_fmt("[%s] ", ts);
}

static void log_plain(const char *fmt, ...)
{
    va_list ap;
    char    buf[4096];

    log_stamp();
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    out_text(buf);
    out_text("\n");
}

static void log_port(int port, const char *fmt, ...)
{
    va_list ap;
    char    buf[4096];

    log_stamp();
    out_fmt("[端口 %d] ", port);
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    out_text(buf);
    out_text("\n");
}

/* ------------------------------------------------------------------ */
/* 基础工具                                                            */
/* ------------------------------------------------------------------ */

static void base64_encode(const unsigned char *in, size_t n, char *out)
{
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i, o = 0;

    for (i = 0; i + 2 < n; i += 3) {
        unsigned v = (in[i] << 16) | (in[i + 1] << 8) | in[i + 2];
        out[o++] = tbl[(v >> 18) & 0x3F];
        out[o++] = tbl[(v >> 12) & 0x3F];
        out[o++] = tbl[(v >> 6) & 0x3F];
        out[o++] = tbl[v & 0x3F];
    }
    if (i < n) {
        unsigned v = in[i] << 16;
        int rest = (int)(n - i);
        if (rest == 2) v |= in[i + 1] << 8;
        out[o++] = tbl[(v >> 18) & 0x3F];
        out[o++] = tbl[(v >> 12) & 0x3F];
        out[o++] = (rest == 2) ? tbl[(v >> 6) & 0x3F] : '=';
        out[o++] = '=';
    }
    out[o] = '\0';
}

static void random_bytes(unsigned char *buf, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++)
        buf[i] = (unsigned char)(rand() & 0xFF);
}

/* 带超时的 TCP 连接；成功返回套接字（阻塞模式），失败返回 INVALID_SOCKET */
static SOCKET tcp_connect(const char *host, int port, int timeout_ms, int *err_out)
{
    SOCKET             s;
    struct sockaddr_in addr;
    u_long             nonblock = 1;
    int                r, so_err = 0, so_len = sizeof so_err;

    if (err_out) *err_out = 0;

    s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
        if (err_out) *err_out = WSAGetLastError();
        return INVALID_SOCKET;
    }

    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((unsigned short)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        closesocket(s);
        if (err_out) *err_out = WSAEINVAL;
        return INVALID_SOCKET;
    }

    ioctlsocket(s, FIONBIO, &nonblock);
    r = connect(s, (struct sockaddr *)&addr, sizeof addr);
    if (r == SOCKET_ERROR) {
        int e = WSAGetLastError();
        if (e != WSAEWOULDBLOCK && e != WSAEINPROGRESS) {
            closesocket(s);
            if (err_out) *err_out = e;
            return INVALID_SOCKET;
        }
        {
            fd_set         wf;
            struct timeval tv;

            FD_ZERO(&wf);
            FD_SET(s, &wf);
            tv.tv_sec  = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;

            r = select(0, NULL, &wf, NULL, &tv);
            if (r <= 0) {
                closesocket(s);
                if (err_out) *err_out = WSAETIMEDOUT;
                return INVALID_SOCKET;
            }
            if (getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&so_err, &so_len) == SOCKET_ERROR
                || so_err != 0) {
                closesocket(s);
                if (err_out) *err_out = so_err ? so_err : WSAGetLastError();
                return INVALID_SOCKET;
            }
        }
    }

    nonblock = 0;
    ioctlsocket(s, FIONBIO, &nonblock);

    /* 关闭 Nagle：回复只有几十字节，避免被攒包延迟几十毫秒 */
    {
        int one = 1;

        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char *)&one, sizeof one);
    }
    return s;
}

/* 探测端口是否可用（只做 TCP 连接测试，随后立即关闭） */
static int probe_port(int port)
{
    SOCKET s = tcp_connect(HOST, port, PROBE_TIMEOUT_MS, NULL);

    if (s == INVALID_SOCKET)
        return 0;
    closesocket(s);
    return 1;
}

/* ------------------------------------------------------------------ */
/* WebSocket                                                           */
/* ------------------------------------------------------------------ */

static int send_all(SOCKET s, const char *data, size_t len)
{
    size_t sent = 0;

    while (sent < len) {
        int n = send(s, data + sent, (int)(len - sent), 0);
        if (n <= 0)
            return 0;
        sent += (size_t)n;
    }
    return 1;
}

/* 完成握手；成功返回 1，失败时把原因写入 reason */
static int ws_handshake(SOCKET s, int port, char *reason, size_t reasonsz)
{
    unsigned char rnd[16];
    char          key[32];
    char          req[512];
    char          resp[2048];
    size_t        used = 0;
    int           len;
    DWORD         start;

    reason[0] = '\0';
    random_bytes(rnd, sizeof rnd);
    base64_encode(rnd, sizeof rnd, key);

    len = snprintf(req, sizeof req,
                   "GET %s HTTP/1.1\r\n"
                   "Host: %s:%d\r\n"
                   "Upgrade: websocket\r\n"
                   "Connection: Upgrade\r\n"
                   "Sec-WebSocket-Key: %s\r\n"
                   "Sec-WebSocket-Version: 13\r\n"
                   "\r\n",
                   "/", HOST, port, key);
    if (len <= 0 || !send_all(s, req, (size_t)len)) {
        snprintf(reason, reasonsz, "握手失败: %s:%d", HOST, port);
        return 0;
    }

    start = GetTickCount();
    while (used < sizeof resp - 1) {
        fd_set         rf;
        struct timeval tv;
        int            r;

        FD_ZERO(&rf);
        FD_SET(s, &rf);
        tv.tv_sec  = 0;
        tv.tv_usec = 200 * 1000;
        r = select(0, &rf, NULL, NULL, &tv);
        if (r < 0) {
            snprintf(reason, reasonsz, "握手失败: %s:%d", HOST, port);
            return 0;
        }
        if (r == 0) {
            if (GetTickCount() - start > 3000) {
                snprintf(reason, reasonsz, "握手超时: %s:%d", HOST, port);
                return 0;
            }
            continue;
        }

        len = recv(s, resp + used, (int)(sizeof resp - 1 - used), 0);
        if (len <= 0) {
            snprintf(reason, reasonsz, "握手失败: %s:%d", HOST, port);
            return 0;
        }
        used += (size_t)len;
        resp[used] = '\0';

        if (strstr(resp, "\r\n\r\n") != NULL)
            break;
    }
    resp[used] = '\0';

    if (strstr(resp, " 101 ") == NULL && strncmp(resp, "HTTP/1.1 101", 12) != 0) {
        char   line[200];
        size_t i = 0;

        while (resp[i] && resp[i] != '\r' && resp[i] != '\n' && i + 1 < sizeof line) {
            line[i] = resp[i];
            i++;
        }
        line[i] = '\0';
        snprintf(reason, reasonsz, "握手被拒绝: %s", line);
        return 0;
    }
    return 1;
}

static int ws_send_text(SOCKET s, const char *text)
{
    size_t        n = strlen(text);
    unsigned char frame[14];
    unsigned char mask[4];
    size_t        hn;
    size_t        i;

    frame[0] = 0x81;                     /* FIN + text */
    if (n < 126) {
        frame[1] = (unsigned char)(0x80 | n);
        hn = 2;
    } else if (n < 65536) {
        frame[1] = (unsigned char)(0x80 | 126);
        frame[2] = (unsigned char)((n >> 8) & 0xFF);
        frame[3] = (unsigned char)(n & 0xFF);
        hn = 4;
    } else {
        frame[1] = (unsigned char)(0x80 | 127);
        for (i = 0; i < 8; i++)
            frame[2 + i] = (unsigned char)((n >> (56 - 8 * i)) & 0xFF);
        hn = 10;
    }

    random_bytes(mask, sizeof mask);
    memcpy(frame + hn, mask, 4);
    hn += 4;

    if (!send_all(s, (const char *)frame, hn))
        return 0;

    /* 分块加掩码发送，避免为大消息再申请缓冲区 */
    {
        char   chunk[1024];
        size_t off = 0;

        while (off < n) {
            size_t take = n - off;
            size_t k;

            if (take > sizeof chunk)
                take = sizeof chunk;
            for (k = 0; k < take; k++)
                chunk[k] = (char)(text[off + k] ^ mask[(off + k) & 3]);
            if (!send_all(s, chunk, take))
                return 0;
            off += take;
        }
    }
    return 1;
}

/* ------------------------------------------------------------------ */
/* 极简 JSON 取值                                                      */
/* ------------------------------------------------------------------ */

static const char *json_find_value(const char *json, const char *key)
{
    char        pat[64];
    const char *p;

    snprintf(pat, sizeof pat, "\"%s\"", key);
    p = strstr(json, pat);
    if (p == NULL)
        return NULL;

    p += strlen(pat);
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;
    if (*p != ':')
        return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;
    return p;
}

/* 取字符串或数字字段，统一转成文本；成功返回 1 */
static int json_get_token(const char *json, const char *key, char *out, size_t outsz)
{
    const char *p;
    size_t      i = 0;

    if (outsz == 0)
        return 0;
    out[0] = '\0';

    p = json_find_value(json, key);
    if (p == NULL)
        return 0;

    if (*p == '"') {
        p++;
        while (*p && *p != '"') {
            if (*p == '\\' && p[1]) {
                p++;
                if (*p == 'u' && isxdigit((unsigned char)p[1]) && isxdigit((unsigned char)p[2])
                    && isxdigit((unsigned char)p[3]) && isxdigit((unsigned char)p[4])) {
                    unsigned cp = 0;
                    int      k;

                    for (k = 1; k <= 4; k++) {
                        char ch = p[k];
                        cp <<= 4;
                        if (ch >= '0' && ch <= '9')      cp |= (unsigned)(ch - '0');
                        else if (ch >= 'a' && ch <= 'f') cp |= (unsigned)(ch - 'a' + 10);
                        else if (ch >= 'A' && ch <= 'F') cp |= (unsigned)(ch - 'A' + 10);
                    }
                    p += 4;

                    /* 代理对：\uD83D\uDE00 -> U+1F600（4 字节 UTF-8，如 emoji）；
                       未配对的代理编码为替换符 U+FFFD，避免产出非法 UTF-8 */
                    if (cp >= 0xD800 && cp <= 0xDBFF
                        && p[1] == '\\' && p[2] == 'u'
                        && isxdigit((unsigned char)p[3]) && isxdigit((unsigned char)p[4])
                        && isxdigit((unsigned char)p[5]) && isxdigit((unsigned char)p[6])) {
                        unsigned lo = 0;

                        for (k = 3; k <= 6; k++) {
                            char ch = p[k];
                            lo <<= 4;
                            if (ch >= '0' && ch <= '9')      lo |= (unsigned)(ch - '0');
                            else if (ch >= 'a' && ch <= 'f') lo |= (unsigned)(ch - 'a' + 10);
                            else if (ch >= 'A' && ch <= 'F') lo |= (unsigned)(ch - 'A' + 10);
                        }
                        if (lo >= 0xDC00 && lo <= 0xDFFF) {
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            p += 6;
                        } else {
                            cp = 0xFFFD;
                        }
                    } else if (cp >= 0xD800 && cp <= 0xDFFF) {
                        cp = 0xFFFD;
                    }

                    if (cp < 0x80 && i + 1 < outsz) {
                        out[i++] = (char)cp;
                    } else if (cp < 0x800 && i + 2 < outsz) {
                        out[i++] = (char)(0xC0 | (cp >> 6));
                        out[i++] = (char)(0x80 | (cp & 0x3F));
                    } else if (cp < 0x10000 && i + 3 < outsz) {
                        out[i++] = (char)(0xE0 | (cp >> 12));
                        out[i++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                        out[i++] = (char)(0x80 | (cp & 0x3F));
                    } else if (i + 4 < outsz) {
                        out[i++] = (char)(0xF0 | (cp >> 18));
                        out[i++] = (char)(0x80 | ((cp >> 12) & 0x3F));
                        out[i++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                        out[i++] = (char)(0x80 | (cp & 0x3F));
                    }
                } else {
                    if (i + 1 >= outsz)
                        break;
                    switch (*p) {
                    case 'n': out[i++] = '\n'; break;
                    case 't': out[i++] = '\t'; break;
                    case 'r': out[i++] = '\r'; break;
                    case 'b': out[i++] = '\b'; break;
                    case 'f': out[i++] = '\f'; break;
                    default:  out[i++] = *p;   break;
                    }
                }
                p++;
            } else {
                if (i + 1 >= outsz)
                    break;
                out[i++] = *p++;
            }
        }
    } else {
        while (*p && *p != ',' && *p != '}' && *p != ' ' && *p != '\r' && *p != '\n') {
            if (i + 1 >= outsz)
                break;
            out[i++] = *p++;
        }
    }

    out[i] = '\0';
    return 1;
}

/* ------------------------------------------------------------------ */
/* 禁言检测（在原有消息判定/回复逻辑之上新增，不改动原有判定逻辑）     */
/*                                                                     */
/*   1) 发送回复时带上 echo，匹配 Snowluma 回的 API 响应；             */
/*      status 为 failed（疑似被禁言）即进入「等待解禁通知」状态。     */
/*   2) 等待期间不做任何轮询/补发，只等 Snowluma 的 group_ban          */
/*      解禁通知（本账号或全员）；收到后立即回复一次。                 */
/*   3) 超过 2 分钟仍未收到解禁通知则放弃本次回复。                    */
/* ------------------------------------------------------------------ */

static void conn_drop(Conn *c, const char *reason);
static void handle_message(Conn *c, const char *json);

/* 发送回复动作帧（带 echo 以便匹配 OneBot 的执行结果） */
static int reply_send(Conn *c)
{
    char reply[192];

    snprintf(c->reply_echo, sizeof c->reply_echo,
             "r%lu", (unsigned long)(++g_echo_seq));
    snprintf(reply, sizeof reply,
             "{\"action\":\"send_group_msg\",\"params\":{\"group_id\":%lld,"
             "\"message\":\"%s\"},\"echo\":\"%s\"}",
             g_group_id, c->trig_reply, c->reply_echo);
    if (!ws_send_text(c->sock, reply))
        return 0;
    c->awaiting       = 1;
    c->send_attempts++;
    c->last_send_tick = GetTickCount64();
    return 1;
}

/* 处理 send_group_msg 的 OneBot 响应：成功才进入冷却；被禁言则进入轮询 */
static void handle_reply_response(Conn *c, const char *json)
{
    char   status[24];
    char   retbuf[24];
    char   wording[256];
    int    ok, was_poll;
    time_t now = time(NULL);

    c->awaiting = 0;
    status[0] = retbuf[0] = '\0';
    json_get_token(json, "status", status, sizeof status);
    json_get_token(json, "retcode", retbuf, sizeof retbuf);

    ok = (strcmp(status, "ok") == 0) || (strcmp(status, "async") == 0)
         || (!status[0] && retbuf[0] && strtoll(retbuf, NULL, 10) == 0);

    if (ok) {
        was_poll = c->mute_poll;
        c->pending_reply  = 0;
        c->mute_poll      = 0;
        c->cooldown_until = now + REPLY_COOLDOWN;
        if (was_poll)
            log_port(c->port,
                     "禁言已解除，已立即回复 \"%s\"（轮询耗时 %.1f 秒），进入 1 分钟冷却",
                     c->trig_reply,
                     (double)(GetTickCount64() - c->poll_start) / 1000.0);
        else
            log_port(c->port,
                     "群成员 %s 消息含 \"扣%s\"，已回复 \"%s\"，进入 1 分钟冷却",
                     c->trig_user, c->trig_reply, c->trig_reply);
        return;
    }

    /* 失败：取出 Snowluma 返回的原因（wording 优先，其次 message） */
    wording[0] = '\0';
    if (!json_get_token(json, "wording", wording, sizeof wording) || !wording[0])
        json_get_token(json, "message", wording, sizeof wording);

    if (!c->mute_poll) {
        int mute = (strstr(wording, "禁言") != NULL);

        c->mute_poll       = 1;
        c->poll_start      = GetTickCount64();
        c->mute_poll_until = c->poll_start
                           + (unsigned long long)MUTE_POLL_SECONDS * 1000ULL;
        log_port(c->port,
                 "回复被拒绝（%s）：%s，不再轮询，等待 Snowluma 的解除禁言通知（%d 分钟内收到才回复）",
                 mute ? "账号被禁言" : "疑似被禁言/发送失败",
                 wording[0] ? wording : "原因未知",
                 MUTE_POLL_SECONDS / 60);
    }
    /* 等待期间的后续失败不再刷屏 */
}

/* 群禁言通知：仅用于日志 + 轮询时的「解除即发」最快路径 */
static void handle_notice(Conn *c, const char *json)
{
    char      ntype[32];
    char      gid[32];
    char      uid[32];
    char      dur[16];
    long long g;
    int       duration;

    if (!json_get_token(json, "notice_type", ntype, sizeof ntype))
        return;
    if (strcmp(ntype, "group_ban") != 0)
        return;
    if (!json_get_token(json, "group_id", gid, sizeof gid))
        return;
    g = strtoll(gid, NULL, 10);
    if (g != g_group_id)
        return;
    if (!json_get_token(json, "user_id", uid, sizeof uid))
        return;
    dur[0] = '\0';
    json_get_token(json, "duration", dur, sizeof dur);
    duration = atoi(dur);

    /* user_id 等于机器人自身，或 0（全员禁言）时，才与「发不出消息」相关 */
    if (!uid[0] || (strcmp(uid, c->self_id) != 0 && strcmp(uid, "0") != 0))
        return;

    if (duration > 0) {
        log_port(c->port, "收到禁言通知：本账号被禁言 %d 秒%s",
                 duration, strcmp(uid, "0") == 0 ? "（全员禁言）" : "");
    } else {
        log_port(c->port, "收到通知：本账号禁言已解除");
        if (c->mute_poll && !c->awaiting
            && c->send_attempts < MAX_SEND_ATTEMPTS) {
            /* 解禁通知到达：立即回复一次，等待结束 */
            c->mute_poll = 0;
            if (!reply_send(c))
                conn_drop(c, "发送回复失败");
        }
        /* 若上一条回复仍在等响应：等它的结果，失败会重新进入等待 */
    }
}

/* 分派一帧：API 响应（带 echo）走响应处理，其余走消息/通知处理 */
static void handle_frame(Conn *c, const char *json)
{
    char echo[32];

    if (c->pending_reply && json_get_token(json, "echo", echo, sizeof echo)
        && strcmp(echo, c->reply_echo) == 0) {
        handle_reply_response(c, json);
        return;
    }
    handle_message(c, json);
}

/* ------------------------------------------------------------------ */
/* 消息处理（逻辑与原单端口版本一致）                                  */
/* ------------------------------------------------------------------ */

static void handle_message(Conn *c, const char *json)
{
    char      post_type[32];
    char      message_type[32];
    char      group_buf[32];
    char      user_buf[64];
    char      raw[4096];
    char      sid[32];
    long long group_id;
    time_t    now;

    if (!json_get_token(json, "post_type", post_type, sizeof post_type))
        return;

    /* 记录机器人自身 QQ（供群禁言通知判断「被禁言的是否是自己」） */
    if (json_get_token(json, "self_id", sid, sizeof sid))
        snprintf(c->self_id, sizeof c->self_id, "%s", sid);

    /* 群禁言通知 */
    if (strcmp(post_type, "notice") == 0) {
        handle_notice(c, json);
        return;
    }
    if (strcmp(post_type, "message") != 0)
        return;

    if (!json_get_token(json, "message_type", message_type, sizeof message_type))
        return;
    if (strcmp(message_type, "group") != 0)
        return;

    if (!json_get_token(json, "group_id", group_buf, sizeof group_buf))
        return;
    group_id = strtoll(group_buf, NULL, 10);
    if (group_id != g_group_id)
        return;

    if (!json_get_token(json, "raw_message", raw, sizeof raw))
        return;

    /* 只监听指定成员的消息，其他人完全静默忽略（不打日志） */
    if (!json_get_token(json, "user_id", user_buf, sizeof user_buf))
        return;
    if (strcmp(user_buf, TARGET_QQ) != 0)
        return;

    /* 在消息中找「扣X」：X = 「扣」后的第一个字（任意字，UTF-8 感知），
       X 即本次回复内容。多个「扣」取第一个可用的。 */
    {
        static const char KOU[] = "\xE6\x89\xA3";   /* 「扣」的 UTF-8 编码 */
        const char       *p    = raw;
        int               found = 0;
        size_t            rawlen = strlen(raw);

        while ((p = strstr(p, KOU)) != NULL) {
            const char   *q = p + 3;
            unsigned char b;
            int           clen;

            /* 跳过半角空格 / 制表符 / 全角空格（U+3000） */
            while (*q == ' ' || *q == '\t'
                   || ((unsigned char)q[0] == 0xE3
                       && (unsigned char)q[1] == 0x80
                       && (unsigned char)q[2] == 0x80))
                q += (*q == ' ' || *q == '\t') ? 1 : 3;

            if (*q == '\0') {           /* 「扣」在句尾，无后续字 */
                p += 3;
                continue;
            }
            b    = (unsigned char)*q;
            clen = (b < 0x80) ? 1 : (b < 0xE0) ? 2 : (b < 0xF0) ? 3 : 4;
            if ((size_t)(q - raw) + (size_t)clen > rawlen) {
                p += 3;                 /* 截断的不完整字符 */
                continue;
            }
            if (clen == 1 && (*q == '"' || *q == '\\')) {
                p += 3;                 /* JSON 特殊字符不作为回复内容 */
                continue;
            }
            memcpy(c->trig_reply, q, (size_t)clen);
            c->trig_reply[clen] = '\0';
            found = 1;
            break;
        }
        if (!found)
            return;                     /* 没有可回复的「扣X」 */
    }

    now = time(NULL);
    if (c->cooldown_until > now) {
        log_port(c->port, "群成员 %s 消息含 \"扣%s\"，冷却中（剩余 %d 秒），忽略",
                 user_buf, c->trig_reply, (int)(c->cooldown_until - now));
        return;
    }

    /* 已有待决回复（含禁言轮询）时不重复触发 */
    if (c->pending_reply) {
        if (c->mute_poll)
            log_port(c->port, "等待解禁通知中，忽略本次触发");
        return;
    }

    /* 发送回复（带 echo，以便匹配 OneBot 的执行结果）。
       冷却与「已回复」日志放到收到响应后再提交：只有 Snowluma 真正
       接受（未被禁言）才算回复成功，避免「以为发了其实被禁言」的假成功。 */
    snprintf(c->trig_user, sizeof c->trig_user, "%s", user_buf);
    c->pending_reply = 1;
    c->mute_poll     = 0;
    c->send_attempts = 0;
    if (!reply_send(c)) {
        c->pending_reply = 0;
        log_port(c->port, "发送回复失败，连接可能已断开");
        return;
    }
}

/* ------------------------------------------------------------------ */
/* 连接管理                                                            */
/* ------------------------------------------------------------------ */

static void conn_drop(Conn *c, const char *reason)
{
    if (c->sock != INVALID_SOCKET) {
        closesocket(c->sock);
        c->sock = INVALID_SOCKET;
    }
    c->connected      = 0;
    c->len            = 0;
    c->next_try       = GetTickCount64() + (unsigned long long)RECONNECT_DELAY * 1000ULL;
    c->pending_reply  = 0;
    c->mute_poll      = 0;
    c->awaiting       = 0;
    if (reason)
        log_port(c->port, "%s，%d 秒后重连...", reason, RECONNECT_DELAY);
}

static int conn_try_connect(Conn *c)
{
    SOCKET s;
    char   reason[256];

    log_port(c->port, "正在连接 ws://%s:%d ...", HOST, c->port);

    s = tcp_connect(HOST, c->port, CONNECT_TIMEOUT_MS, NULL);
    if (s == INVALID_SOCKET) {
        snprintf(reason, sizeof reason, "无法连接 %s:%d", HOST, c->port);
        c->next_try = GetTickCount64() + (unsigned long long)RECONNECT_DELAY * 1000ULL;
        log_port(c->port, "%s，%d 秒后重连...", reason, RECONNECT_DELAY);
        return 0;
    }

    if (!ws_handshake(s, c->port, reason, sizeof reason)) {
        closesocket(s);
        c->next_try = GetTickCount64() + (unsigned long long)RECONNECT_DELAY * 1000ULL;
        log_port(c->port, "%s，%d 秒后重连...", reason, RECONNECT_DELAY);
        return 0;
    }

    c->sock      = s;
    c->connected = 1;
    c->len       = 0;
    log_port(c->port, "已连接 Snowluma，开始监听群 %lld", g_group_id);
    return 1;
}

/* 解析接收缓冲中的完整帧 */
static void conn_parse_frames(Conn *c)
{
    while (c->len >= 2) {
        unsigned char *b = (unsigned char *)c->buf;
        int            opcode = b[0] & 0x0F;
        int            masked = (b[1] & 0x80) ? 1 : 0;
        unsigned long long plen = b[1] & 0x7F;
        size_t         hdr = 2;
        size_t         i;

        if (plen == 126) {
            if (c->len < 4)
                return;
            plen = ((unsigned long long)b[2] << 8) | b[3];
            hdr  = 4;
        } else if (plen == 127) {
            if (c->len < 10)
                return;
            plen = 0;
            for (i = 0; i < 8; i++)
                plen = (plen << 8) | b[2 + i];
            hdr = 10;
        }
        if (masked)
            hdr += 4;

        /* 单帧超过接收缓冲（异常/超长数据）：直接断开重连，避免越界或卡死 */
        if (plen > (unsigned long long)(sizeof c->buf - hdr)) {
            conn_drop(c, "连接被断开");
            return;
        }
        if (c->len < hdr + (size_t)plen)
            return;

        if (masked) {
            unsigned char *mk = b + hdr - 4;
            for (i = 0; i < (size_t)plen; i++)
                b[hdr + i] ^= mk[i & 3];
        }

        if (opcode == 0x1 || opcode == 0x2) {
            static char msg[RECV_BUF];

            if ((size_t)plen < sizeof msg) {
                memcpy(msg, c->buf + hdr, (size_t)plen);
                msg[plen] = '\0';
                handle_frame(c, msg);
            }
        } else if (opcode == 0x8) {
            conn_drop(c, "连接被断开");
            return;
        } else if (opcode == 0x9) {
            /* ping -> pong */
            unsigned char pong[2] = { 0x8A, 0x80 };
            unsigned char mk[4];

            random_bytes(mk, 4);
            if (send_all(c->sock, (const char *)pong, 2)
                && send_all(c->sock, (const char *)mk, 4)) {
                /* 空载荷 pong 已足够 */
            }
        }

        memmove(c->buf, c->buf + hdr + (size_t)plen, c->len - hdr - (size_t)plen);
        c->len -= hdr + (size_t)plen;
    }
}

static void conn_read(Conn *c)
{
    int n;

    n = recv(c->sock, c->buf + c->len, (int)(sizeof c->buf - c->len), 0);
    if (n == 0) {
        conn_drop(c, "连接被断开");
        return;
    }
    if (n < 0) {
        int e = WSAGetLastError();

        if (e == WSAEWOULDBLOCK)
            return;
        conn_drop(c, "连接被断开");
        return;
    }

    c->len += (size_t)n;
    conn_parse_frames(c);
}

/* ------------------------------------------------------------------ */
/* 启动扫描                                                            */
/* ------------------------------------------------------------------ */

static int scan_ports(int *available_ports, int *avail_count)
{
    int i;
    int unavailable[PORT_COUNT];
    int unavail_count = 0;

    *avail_count = 0;

    log_plain("开始扫描端口 %d - %d ...", g_ports[0], g_ports[PORT_COUNT - 1]);

    for (i = 0; i < PORT_COUNT; i++) {
        int port = g_ports[i];

        g_conns[i].port           = port;
        g_conns[i].sock           = INVALID_SOCKET;
        g_conns[i].connected      = 0;
        g_conns[i].len            = 0;
        g_conns[i].next_try       = 0;
        g_conns[i].cooldown_until = 0;
        g_conns[i].pending_reply  = 0;
        g_conns[i].awaiting       = 0;
        g_conns[i].reply_echo[0]  = '\0';
        g_conns[i].trig_user[0]   = '\0';
        g_conns[i].trig_reply[0]  = '\0';
        g_conns[i].mute_poll      = 0;
        g_conns[i].poll_start     = 0;
        g_conns[i].mute_poll_until= 0;
        g_conns[i].next_poll      = 0;
        g_conns[i].last_send_tick = 0;
        g_conns[i].self_id[0]     = '\0';
        g_conns[i].send_attempts  = 0;
        g_conns[i].available      = probe_port(port);

        if (g_conns[i].available) {
            available_ports[(*avail_count)++] = port;
            log_plain("端口 %d 可用，将加入监听", port);
        } else {
            unavailable[unavail_count++] = port;
            log_plain("端口 %d 不可用，本次不监听（不会重连）", port);
        }
    }

    {
        char buf_a[64] = "";
        char buf_u[64] = "";

        for (i = 0; i < *avail_count; i++) {
            char tmp[12];

            snprintf(tmp, sizeof tmp, "%s%d", i ? "、" : "", available_ports[i]);
            strncat(buf_a, tmp, sizeof buf_a - strlen(buf_a) - 1);
        }
        for (i = 0; i < unavail_count; i++) {
            char tmp[12];

            snprintf(tmp, sizeof tmp, "%s%d", i ? "、" : "", unavailable[i]);
            strncat(buf_u, tmp, sizeof buf_u - strlen(buf_u) - 1);
        }

        log_plain("端口扫描完成：可用 %d 个 [%s]，不可用 %d 个 [%s]",
                  *avail_count, buf_a[0] ? buf_a : "无",
                  unavail_count, buf_u[0] ? buf_u : "无");
    }

    return *avail_count;
}

/* ------------------------------------------------------------------ */
/* 主程序                                                              */
/* ------------------------------------------------------------------ */

static BOOL WINAPI ctrl_handler(DWORD type)
{
    (void)type;
    g_running = 0;
    return TRUE;
}

static long long ask_group_id(void)
{
    char line[256];

    for (;;) {
        size_t i;
        int    ok = 1;

        out_text("请输入要监听的群号: ");

        in_prepare();
        if (!in_read_line(line, sizeof line)) {
            in_restore();
            g_running = 0;
            return -1;
        }
        in_restore();

        {
            size_t n = strlen(line);

            while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r'
                             || line[n - 1] == ' ' || line[n - 1] == '\t')) {
                line[--n] = '\0';
            }
        }

        if (line[0] == '\0')
            ok = 0;
        for (i = 0; line[i] && ok; i++) {
            if (!isdigit((unsigned char)line[i]))
                ok = 0;
        }

        if (!ok) {
            out_text("群号必须是纯数字，请重新输入。\n");
            continue;
        }
        return strtoll(line, NULL, 10);
    }
}

int main(void)
{
    WSADATA     wsa;
    int         available_ports[PORT_COUNT];
    int         avail_count = 0;
    int         i;
    unsigned    seed;

    out_init();
    in_init();
    SetConsoleCtrlHandler(ctrl_handler, TRUE);
    setvbuf(stdout, NULL, _IONBF, 0);
    seed = (unsigned)GetTickCount64() ^ (unsigned)GetCurrentProcessId();
    srand(seed);

    out_text("==================================================\n");
    out_text("  Snowluma 群消息自动回复工具（C 语言版）V3.0.0\n");
    out_text("==================================================\n");
    out_text("  功能说明：\n");
    out_text("   - 自动连接本机 Snowluma (正向 WebSocket)\n");
    out_fmt("   - 同时监听 %d 个端口（%d / %d / %d / %d）\n", PORT_COUNT,
            g_ports[0], g_ports[1], g_ports[2], g_ports[3]);
    out_text("   - 启动时扫描可用端口，不可用端口仅提示、不重连\n");
    out_text("   - 仅监听指定群聊中指定成员（QQ " TARGET_QQ "）的消息\n");
    out_text("   - 该成员消息含「扣X」（X 任意字）时，立即回复 X 本身\n");
    out_text("   - 触发后该端口（账号）冷却 1 分钟，各端口互不影响\n");
    out_text("   - 回复被拒（如被禁言）时：不轮询，等待解禁通知，2 分钟内收到才回复\n");
    out_text("   - 可用端口断线自动重连\n");
    out_fmt("  监听端口：%d / %d / %d / %d\n",
            g_ports[0], g_ports[1], g_ports[2], g_ports[3]);
    out_text("==================================================\n");
    out_text("\n");

    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        out_text("WSAStartup 失败，无法初始化网络。\n");
        return 1;
    }

    if (scan_ports(available_ports, &avail_count) == 0) {
        log_plain("没有任何可用端口，程序退出。请先启动 Snowluma 后重新运行。");
        WSACleanup();
        return 1;
    }

    g_group_id = ask_group_id();
    if (g_group_id < 0 || !g_running) {
        log_plain("已退出。");
        WSACleanup();
        return 0;
    }
    log_plain("已设定监听群号：%lld", g_group_id);
    log_plain("程序运行中，按 Ctrl+C 退出。");
    out_text("\n");

    while (g_running) {
        unsigned long long now = GetTickCount64();
        fd_set             rf;
        struct timeval     tv;
        int                any = 0;
        int                r;

        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (!c->available || c->connected)
                continue;
            if (now < c->next_try)
                continue;
            conn_try_connect(c);
        }

        /* 回复结果处理：禁言轮询 / 响应超时兜底 */
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (!c->available || !c->connected || !c->pending_reply)
                continue;

            if (c->mute_poll) {
                /* 只等待 Snowluma 的解除禁言通知（handle_notice 处理），
                   不做任何轮询 / 补发；超过窗口放弃 */
                if (now >= c->mute_poll_until) {
                    c->mute_poll     = 0;
                    c->pending_reply = 0;
                    c->awaiting      = 0;
                    log_port(c->port,
                             "%d 分钟内未收到解除禁言通知，放弃本次回复（不进入冷却，下次触发重新尝试）",
                             MUTE_POLL_SECONDS / 60);
                    continue;
                }
            } else if (now - c->last_send_tick >= 2000) {
                /* 响应迟迟未到（罕见）：按原语义视为已发出 */
                c->pending_reply  = 0;
                c->awaiting       = 0;
                c->cooldown_until = time(NULL) + REPLY_COOLDOWN;
                log_port(c->port,
                         "群成员 %s 消息含 \"扣%s\"，已回复 \"%s\"，进入 1 分钟冷却",
                         c->trig_user, c->trig_reply, c->trig_reply);
            }
        }

        FD_ZERO(&rf);
        for (i = 0; i < PORT_COUNT; i++) {
            if (g_conns[i].available && g_conns[i].connected) {
                FD_SET(g_conns[i].sock, &rf);
                any = 1;
            }
        }

        if (!any) {
            Sleep(100);
            continue;
        }

        tv.tv_sec  = 0;
        tv.tv_usec = SELECT_TIMEOUT_MS * 1000;
        r = select(0, &rf, NULL, NULL, &tv);
        if (r <= 0)
            continue;

        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (!c->available || !c->connected)
                continue;
            if (!FD_ISSET(c->sock, &rf))
                continue;
            conn_read(c);
        }
    }

    for (i = 0; i < PORT_COUNT; i++) {
        if (g_conns[i].sock != INVALID_SOCKET) {
            closesocket(g_conns[i].sock);
            g_conns[i].sock = INVALID_SOCKET;
        }
    }
    WSACleanup();

    log_plain("已退出。");
    return 0;
}
