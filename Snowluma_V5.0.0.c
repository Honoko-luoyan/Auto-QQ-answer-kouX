/*
 * Snowluma 三端口数字跟发与「扣数字」回复工具（C 语言版）V5.0.0
 *
 * 规则：
 *   1. 仅 3001 接收并判断群消息；3002/3003 只负责回复。
 *   2. 指定群内，TARGET_QQ 发送图片或含 @全体的消息后开启唯一的一份全局开关。
 *      开关已开启且尚未触发时，新的图片或 @全体消息都会清空配对表，并以最后
 *      一条触发消息的处理时间重新计时 120 秒。
 *   3. 开关开启期间，只处理一位纯数字消息（0~9）。两个不同 QQ 在 30 秒内发送
 *      相同数字时，立即关闭全局开关，并尽快通过启动时可用且当前已连接的端口回复。
 *      同一用户重复发送不计且不延长窗口；超过 30 秒后由当前消息重新作为首条记录。
 *      黑名单成员发送的数字不参与配对；其任意新鲜群消息都会立即关闭全局开关。
 *   4. 回复后无论耗时是否已满 120 秒，开关均已关闭；开关关闭时不发送任何消息。
 *   5. 启动时检测端口：3001 不可用会无限重连；3002/3003 启动不可用则本次运行
 *      不监听、不重连、不回复；启动可用但后来断线的端口会自动重连。
 *   6. 禁用控制台快速编辑模式，并使用有界异步日志队列：控制台暂停只影响日志
 *      显示，不会阻塞网络、计时和回复；队列满时丢弃日志而不拖住网络线程。
 *   7. 目标 QQ 的 @全体消息若含「扣」后首个非空白字符为 0~9，
 *      按 V3 规则在 3001 回复该数字；成功后进入 60 秒冷却并关闭 V4 全局开关。
 *      被拒时等待解禁通知，最多 2 分钟；收到解禁通知后立即重发。
 *   8. 丢弃超过 STALE_MESSAGE_SECONDS 秒的相关旧消息，防止使用积压消息补触发；
 *      数字配对回复提交后 5 秒内无 Snowluma 成功确认则放弃等待，不重试、不补发。
 *
 * 构建（Windows）：
 *   MSVC:  cl /utf-8 Snowluma_V5.0.0.c ws2_32.lib
 *   MinGW: gcc -O2 Snowluma_V5.0.0.c -o Snowluma_V5.0.0.exe -lws2_32
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

#define HOST                     "127.0.0.1"
#define GROUP_ID                 1124334011LL
#define GROUP_ID_STR             "1124334011"
#define TARGET_QQ                "3288574505" /* 占位目标 QQ，后续可直接修改此处 */
#define PORT_COUNT               3
#define RECV_BUF                 65536
#define RECONNECT_DELAY          3
#define CONNECT_TIMEOUT_MS       800
#define PROBE_TIMEOUT_MS         400
#define SELECT_TIMEOUT_MS        50
#define FOLLOW_SWITCH_SECONDS    120
#define FOLLOW_MATCH_SECONDS     30
#define STALE_MESSAGE_SECONDS    5
#define REPLY_RESPONSE_TIMEOUT   5000
#define KOU_REPLY_COOLDOWN       60
#define KOU_MUTE_WAIT_SECONDS    120
#define KOU_REPLY_RESPONSE_TIMEOUT 2000
#define KOU_MAX_SEND_ATTEMPTS    20
#define SEND_TIMEOUT_MS          250
#define LOG_QUEUE_SIZE           512
#define LOG_MESSAGE_SIZE         2048
#define CONSOLE_RECHECK_MS       5000

static const int g_ports[PORT_COUNT] = { 3001, 3002, 3003 };

/* 黑名单成员发送的数字不参与跟发；修改名单后需要重新编译。 */
static const char *g_blacklist[] = {
    "3117669520", "2248908690", "202945218"
};

typedef struct {
    int                 port;
    int                 enabled;       /* 本次运行是否允许使用此端口 */
    SOCKET              sock;
    int                 connected;
    unsigned long long  next_try;      /* 下次重连时刻（GetTickCount64） */
    int                 awaiting;      /* 是否等待最近一次 send_group_msg 响应 */
    char                reply_echo[40];
    char                last_digit[2];
    unsigned long long  last_send_tick;
    char                self_id[32];   /* 仅 3001 用于排除自己的消息回显 */
    time_t              kou_cooldown_until;
    int                 kou_pending;
    int                 kou_awaiting;
    char                kou_echo[40];
    char                kou_digit[2];
    char                kou_user[64];
    int                 kou_mute_wait;
    unsigned long long  kou_mute_deadline;
    unsigned long long  kou_last_send_tick;
    int                 kou_send_attempts;
    char                buf[RECV_BUF];
    size_t              len;
} Conn;

typedef struct {
    int                 used;
    char                qq[64];
    unsigned long long  tick; /* 首位记录时间（GetTickCount64） */
} FirstDigit;

static Conn       g_conns[PORT_COUNT];
static FirstDigit g_first_digit[10];  /* 每个数字在本次开关期间的第一位发送者 */
static int        g_follow_active = 0;
static unsigned long long g_follow_deadline = 0;
static volatile int g_running = 1;
static unsigned long g_echo_seq = 0;

/* ------------------------------------------------------------------ */
/* 控制台输出                                                          */
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
        int n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, wbuf,
                                    (int)(sizeof wbuf / sizeof wbuf[0]));

        if (n > 1) {
            DWORD written = 0;

            if (WriteConsoleW(g_out, wbuf, (DWORD)(n - 1), &written, NULL))
                return;

            /* WriteConsoleW 不可用时，按当前控制台代码页输出。 */
            {
                char mb[8192];
                UINT cp = GetConsoleOutputCP();
                int m;

                if (cp == 0)
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
    char buf[4096];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    out_text(buf);
}

/*
 * 异步日志队列：网络线程只复制日志到内存，绝不直接等待控制台。
 * 控制台即使被暂停，也只会阻塞日志线程，不会阻塞接收、计时或回复。
 * 队列有固定上限；极端情况下丢弃部分日志而不是积压内存或拖住网络。
 */
static CRITICAL_SECTION g_log_lock;
static HANDLE g_log_wake = NULL;
static HANDLE g_log_thread = NULL;
static int    g_log_lock_ready = 0;
static int    g_log_async = 0;
static int    g_log_stop = 0;
static char   g_log_queue[LOG_QUEUE_SIZE][LOG_MESSAGE_SIZE];
static size_t g_log_head = 0;
static size_t g_log_count = 0;
static unsigned long g_log_dropped = 0;

static void log_enqueue(const char *text)
{
    if (!g_log_async) {
        out_text(text);
        return;
    }

    EnterCriticalSection(&g_log_lock);
    if (g_log_count < LOG_QUEUE_SIZE) {
        size_t tail = (g_log_head + g_log_count) % LOG_QUEUE_SIZE;

        snprintf(g_log_queue[tail], LOG_MESSAGE_SIZE, "%s", text);
        g_log_count++;
    } else {
        g_log_dropped++;
    }
    LeaveCriticalSection(&g_log_lock);
    SetEvent(g_log_wake);
}

static DWORD WINAPI log_thread_proc(LPVOID unused)
{
    (void)unused;

    for (;;) {
        char text[LOG_MESSAGE_SIZE];
        unsigned long dropped = 0;
        int have_text = 0;
        int stop;

        EnterCriticalSection(&g_log_lock);
        if (g_log_count > 0) {
            memcpy(text, g_log_queue[g_log_head], LOG_MESSAGE_SIZE);
            text[LOG_MESSAGE_SIZE - 1] = '\0';
            g_log_head = (g_log_head + 1) % LOG_QUEUE_SIZE;
            g_log_count--;
            have_text = 1;
            if (g_log_dropped > 0) {
                dropped = g_log_dropped;
                g_log_dropped = 0;
            }
        }
        stop = g_log_stop;
        LeaveCriticalSection(&g_log_lock);

        if (have_text) {
            out_text(text); /* 只允许日志线程在这里等待控制台。 */
            if (dropped > 0) {
                char warning[160];

                snprintf(warning, sizeof warning,
                         "[日志] 控制台阻塞期间已丢弃 %lu 条日志；网络与回复未停止\n",
                         dropped);
                out_text(warning);
            }
        } else {
            if (stop)
                break;
            WaitForSingleObject(g_log_wake, 100);
        }
    }
    return 0;
}

static void log_init(void)
{
    InitializeCriticalSection(&g_log_lock);
    g_log_lock_ready = 1;
    g_log_wake = CreateEvent(NULL, FALSE, FALSE, NULL);
    if (g_log_wake == NULL)
        return;

    g_log_async = 1; /* 在创建线程前发布状态，避免初始化期间出现竞态。 */
    g_log_thread = CreateThread(NULL, 0, log_thread_proc, NULL, 0, NULL);
    if (g_log_thread == NULL) {
        g_log_async = 0;
        CloseHandle(g_log_wake);
        g_log_wake = NULL;
        out_text("警告：异步日志线程创建失败，将使用同步日志。\n");
    }
}

static void log_shutdown(void)
{
    DWORD wait_result;

    if (!g_log_async)
        return;

    EnterCriticalSection(&g_log_lock);
    g_log_stop = 1;
    LeaveCriticalSection(&g_log_lock);
    SetEvent(g_log_wake);

    wait_result = WaitForSingleObject(g_log_thread, 1500);
    if (wait_result != WAIT_OBJECT_0) {
        /* 控制台仍阻塞时不强杀线程、不关闭其句柄；进程随即退出。 */
        return;
    }

    CloseHandle(g_log_thread);
    CloseHandle(g_log_wake);
    g_log_thread = NULL;
    g_log_wake = NULL;
    g_log_async = 0;
    if (g_log_lock_ready) {
        DeleteCriticalSection(&g_log_lock);
        g_log_lock_ready = 0;
    }
}

static void log_format_time(char *ts, size_t tssz)
{
    time_t now = time(NULL);
    struct tm tmv = { 0 };

    localtime_s(&tmv, &now);
    strftime(ts, tssz, "%H:%M:%S", &tmv);
}

static void log_plain(const char *fmt, ...)
{
    va_list ap;
    char body[1600];
    char line[LOG_MESSAGE_SIZE];
    char ts[16];

    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    log_format_time(ts, sizeof ts);
    snprintf(line, sizeof line, "[%s] %s\n", ts, body);
    log_enqueue(line);
}

static void log_port(int port, const char *fmt, ...)
{
    va_list ap;
    char body[1600];
    char line[LOG_MESSAGE_SIZE];
    char ts[16];

    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    log_format_time(ts, sizeof ts);
    snprintf(line, sizeof line, "[%s] [端口 %d] %s\n", ts, port, body);
    log_enqueue(line);
}

/* 防止鼠标点击或选择控制台文本；运行中每 5 秒重新确认一次设置。 */
static void disable_console_quick_edit(int announce)
{
    HANDLE h_in = GetStdHandle(STD_INPUT_HANDLE);
    DWORD mode;

    if (h_in == INVALID_HANDLE_VALUE || h_in == NULL)
        return;
    if (!GetConsoleMode(h_in, &mode))
        return;

    mode &= ~ENABLE_QUICK_EDIT_MODE;
    mode |= ENABLE_EXTENDED_FLAGS;
    if (!announce) {
        SetConsoleMode(h_in, mode);
        return;
    }
    if (SetConsoleMode(h_in, mode))
        log_plain("已关闭控制台快速编辑模式；日志使用独立线程，控制台暂停不会卡住网络");
    else
        log_plain("警告：无法关闭控制台快速编辑模式；异步日志仍会保护网络与回复");
}

/* ------------------------------------------------------------------ */
/* 基础网络工具                                                        */
/* ------------------------------------------------------------------ */

static void base64_encode(const unsigned char *in, size_t n, char *out)
{
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i, o = 0;

    for (i = 0; i + 2 < n; i += 3) {
        unsigned v = ((unsigned)in[i] << 16)
                   | ((unsigned)in[i + 1] << 8) | in[i + 2];
        out[o++] = tbl[(v >> 18) & 0x3F];
        out[o++] = tbl[(v >> 12) & 0x3F];
        out[o++] = tbl[(v >> 6) & 0x3F];
        out[o++] = tbl[v & 0x3F];
    }
    if (i < n) {
        unsigned v = (unsigned)in[i] << 16;
        int rest = (int)(n - i);

        if (rest == 2)
            v |= (unsigned)in[i + 1] << 8;
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

static SOCKET tcp_connect(const char *host, int port, int timeout_ms)
{
    SOCKET s;
    struct sockaddr_in addr;
    u_long nonblock = 1;
    int r, so_err = 0, so_len = sizeof so_err;

    s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET)
        return INVALID_SOCKET;

    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        closesocket(s);
        return INVALID_SOCKET;
    }

    ioctlsocket(s, FIONBIO, &nonblock);
    r = connect(s, (struct sockaddr *)&addr, sizeof addr);
    if (r == SOCKET_ERROR) {
        int e = WSAGetLastError();

        if (e != WSAEWOULDBLOCK && e != WSAEINPROGRESS) {
            closesocket(s);
            return INVALID_SOCKET;
        }
        {
            fd_set wf;
            struct timeval tv;

            FD_ZERO(&wf);
            FD_SET(s, &wf);
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            r = select(0, NULL, &wf, NULL, &tv);
            if (r <= 0) {
                closesocket(s);
                return INVALID_SOCKET;
            }
            if (getsockopt(s, SOL_SOCKET, SO_ERROR,
                           (char *)&so_err, &so_len) == SOCKET_ERROR
                || so_err != 0) {
                closesocket(s);
                return INVALID_SOCKET;
            }
        }
    }

    nonblock = 0;
    ioctlsocket(s, FIONBIO, &nonblock);
    {
        int one = 1;

        DWORD send_timeout = SEND_TIMEOUT_MS;

        setsockopt(s, IPPROTO_TCP, TCP_NODELAY,
                   (const char *)&one, sizeof one);
        /* 本机小数据帧正常会立即发出；异常时也不能无限阻塞其他端口。 */
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO,
                   (const char *)&send_timeout, sizeof send_timeout);
    }
    return s;
}

static int probe_port(int port)
{
    SOCKET s = tcp_connect(HOST, port, PROBE_TIMEOUT_MS);

    if (s == INVALID_SOCKET)
        return 0;
    closesocket(s);
    return 1;
}

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

/* ------------------------------------------------------------------ */
/* WebSocket                                                           */
/* ------------------------------------------------------------------ */

static int ws_handshake(SOCKET s, int port, char *reason, size_t reasonsz)
{
    unsigned char rnd[16];
    char key[32];
    char req[512];
    char resp[2048];
    size_t used = 0;
    int len;
    DWORD start;

    reason[0] = '\0';
    random_bytes(rnd, sizeof rnd);
    base64_encode(rnd, sizeof rnd, key);

    len = snprintf(req, sizeof req,
                   "GET / HTTP/1.1\r\n"
                   "Host: %s:%d\r\n"
                   "Upgrade: websocket\r\n"
                   "Connection: Upgrade\r\n"
                   "Sec-WebSocket-Key: %s\r\n"
                   "Sec-WebSocket-Version: 13\r\n\r\n",
                   HOST, port, key);
    if (len <= 0 || !send_all(s, req, (size_t)len)) {
        snprintf(reason, reasonsz, "WebSocket 握手发送失败");
        return 0;
    }

    start = GetTickCount();
    while (used < sizeof resp - 1) {
        fd_set rf;
        struct timeval tv;
        int r;

        FD_ZERO(&rf);
        FD_SET(s, &rf);
        tv.tv_sec = 0;
        tv.tv_usec = 200 * 1000;
        r = select(0, &rf, NULL, NULL, &tv);
        if (r < 0) {
            snprintf(reason, reasonsz, "WebSocket 握手读取失败");
            return 0;
        }
        if (r == 0) {
            if (GetTickCount() - start > 3000) {
                snprintf(reason, reasonsz, "WebSocket 握手超时");
                return 0;
            }
            continue;
        }

        len = recv(s, resp + used, (int)(sizeof resp - 1 - used), 0);
        if (len <= 0) {
            snprintf(reason, reasonsz, "WebSocket 握手连接被关闭");
            return 0;
        }
        used += (size_t)len;
        resp[used] = '\0';
        if (strstr(resp, "\r\n\r\n") != NULL)
            break;
    }
    resp[used] = '\0';

    if (strstr(resp, " 101 ") == NULL
        && strncmp(resp, "HTTP/1.1 101", 12) != 0) {
        char line[200];
        size_t i = 0;

        while (resp[i] && resp[i] != '\r' && resp[i] != '\n'
               && i + 1 < sizeof line) {
            line[i] = resp[i];
            i++;
        }
        line[i] = '\0';
        snprintf(reason, reasonsz, "WebSocket 握手被拒绝：%s", line);
        return 0;
    }
    return 1;
}

static int ws_send_text(SOCKET s, const char *text)
{
    size_t n = strlen(text);

    /* 回复帧很短：掩码和载荷一次组成完整帧，只调用一次发送路径。 */
    if (n < 126) {
        unsigned char packet[132];
        unsigned char mask[4];
        size_t i;

        packet[0] = 0x81;
        packet[1] = (unsigned char)(0x80 | n);
        random_bytes(mask, sizeof mask);
        memcpy(packet + 2, mask, sizeof mask);
        for (i = 0; i < n; i++)
            packet[6 + i] = (unsigned char)text[i] ^ mask[i & 3];
        return send_all(s, (const char *)packet, 6 + n);
    }

    unsigned char frame[14];
    unsigned char mask[4];
    size_t hn, i;

    frame[0] = 0x81; /* FIN + text */
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
        {
            unsigned long long payload_size = (unsigned long long)n;

            for (i = 0; i < 8; i++)
                frame[2 + i] = (unsigned char)((payload_size >> (56 - 8 * i)) & 0xFF);
        }
        hn = 10;
    }

    random_bytes(mask, sizeof mask);
    memcpy(frame + hn, mask, 4);
    hn += 4;
    if (!send_all(s, (const char *)frame, hn))
        return 0;

    {
        char chunk[1024];
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
    char pat[64];
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

static int json_get_token(const char *json, const char *key,
                          char *out, size_t outsz)
{
    const char *p;
    size_t i = 0;

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
                if (*p == 'u'
                    && isxdigit((unsigned char)p[1])
                    && isxdigit((unsigned char)p[2])
                    && isxdigit((unsigned char)p[3])
                    && isxdigit((unsigned char)p[4])) {
                    unsigned cp = 0;
                    int k;

                    for (k = 1; k <= 4; k++) {
                        char ch = p[k];

                        cp <<= 4;
                        if (ch >= '0' && ch <= '9')
                            cp |= (unsigned)(ch - '0');
                        else if (ch >= 'a' && ch <= 'f')
                            cp |= (unsigned)(ch - 'a' + 10);
                        else
                            cp |= (unsigned)(ch - 'A' + 10);
                    }
                    p += 4;

                    if (cp >= 0xD800 && cp <= 0xDBFF
                        && p[1] == '\\' && p[2] == 'u'
                        && isxdigit((unsigned char)p[3])
                        && isxdigit((unsigned char)p[4])
                        && isxdigit((unsigned char)p[5])
                        && isxdigit((unsigned char)p[6])) {
                        unsigned lo = 0;

                        for (k = 3; k <= 6; k++) {
                            char ch = p[k];

                            lo <<= 4;
                            if (ch >= '0' && ch <= '9')
                                lo |= (unsigned)(ch - '0');
                            else if (ch >= 'a' && ch <= 'f')
                                lo |= (unsigned)(ch - 'a' + 10);
                            else
                                lo |= (unsigned)(ch - 'A' + 10);
                        }
                        if (lo >= 0xDC00 && lo <= 0xDFFF) {
                            cp = 0x10000 + ((cp - 0xD800) << 10)
                                 + (lo - 0xDC00);
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
                    default:  out[i++] = *p; break;
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
        while (*p && *p != ',' && *p != '}' && *p != ' '
               && *p != '\r' && *p != '\n') {
            if (i + 1 >= outsz)
                break;
            out[i++] = *p++;
        }
    }
    out[i] = '\0';
    return 1;
}

/* ------------------------------------------------------------------ */
/* 回复提交与结果确认                                                  */
/* ------------------------------------------------------------------ */

static void conn_drop(Conn *c, const char *reason);
static void handle_message(Conn *c, const char *json);

/*
 * 先连续提交三个端口的发送帧，期间不打印日志、不等待 API 响应，
 * 避免控制台输出或某一端口的响应速度拖慢其他端口。所有发送结束后才统一记日志。
 * 返回值仅用于本函数内部记录：1 已提交，0 发送失败，-1 启动时不可用，-2 当前未连接。
 */
static void reply_submit_all(char digit)
{
    int results[PORT_COUNT];
    int i;

    for (i = 0; i < PORT_COUNT; i++) {
        Conn *c = &g_conns[i];
        char request[256];

        if (!c->enabled) {
            results[i] = -1;
            continue;
        }
        if (!c->connected || c->sock == INVALID_SOCKET) {
            results[i] = -2;
            continue;
        }

        snprintf(c->reply_echo, sizeof c->reply_echo,
                 "p%d-r%lu", c->port, (unsigned long)(++g_echo_seq));
        snprintf(c->last_digit, sizeof c->last_digit, "%c", digit);
        snprintf(request, sizeof request,
                 "{\"action\":\"send_group_msg\",\"params\":{\"group_id\":%lld,"
                 "\"message\":\"%c\"},\"echo\":\"%s\"}",
                 GROUP_ID, digit, c->reply_echo);

        if (ws_send_text(c->sock, request)) {
            c->awaiting = 1;
            c->last_send_tick = GetTickCount64();
            results[i] = 1;
        } else {
            c->awaiting = 0;
            results[i] = 0;
        }
    }

    for (i = 0; i < PORT_COUNT; i++) {
        Conn *c = &g_conns[i];

        if (results[i] == 1) {
            log_port(c->port, "数字 %c 回复帧已提交，等待 Snowluma 确认", digit);
        } else if (results[i] == 0) {
            log_port(c->port, "数字 %c 回复发送失败，不排队、不补发", digit);
            conn_drop(c, "发送回复时连接失败");
        } else if (results[i] == -2) {
            log_port(c->port, "数字 %c 未回复：当前未连接，不排队、不补发", digit);
        } else {
            log_port(c->port, "数字 %c 未回复：启动检测时端口不可用", digit);
        }
    }
}

static void handle_reply_response(Conn *c, const char *json)
{
    char status[24];
    char retbuf[24];
    char wording[256];
    int ok;

    c->awaiting = 0;
    status[0] = retbuf[0] = '\0';
    json_get_token(json, "status", status, sizeof status);
    json_get_token(json, "retcode", retbuf, sizeof retbuf);

    ok = (strcmp(status, "ok") == 0) || (strcmp(status, "async") == 0)
         || (!status[0] && retbuf[0] && strtoll(retbuf, NULL, 10) == 0);
    if (ok) {
        log_port(c->port, "Snowluma 确认数字 %s 回复成功",
                 c->last_digit[0] ? c->last_digit : "?");
        return;
    }

    wording[0] = '\0';
    if (!json_get_token(json, "wording", wording, sizeof wording)
        || !wording[0])
        json_get_token(json, "message", wording, sizeof wording);
    log_port(c->port, "数字 %s 回复被拒绝：%s；不重试、不补发",
             c->last_digit[0] ? c->last_digit : "?",
             wording[0] ? wording : "原因未知");
}

/* V3 的「扣数字」回复只走收到 @全体 消息的 3001 连接。 */
static int kou_reply_send(Conn *c)
{
    char request[256];

    snprintf(c->kou_echo, sizeof c->kou_echo,
             "k%d-r%lu", c->port, (unsigned long)(++g_echo_seq));
    snprintf(request, sizeof request,
             "{\"action\":\"send_group_msg\",\"params\":{\"group_id\":%lld,"
             "\"message\":\"%s\"},\"echo\":\"%s\"}",
             GROUP_ID, c->kou_digit, c->kou_echo);
    if (!ws_send_text(c->sock, request))
        return 0;

    c->kou_awaiting = 1;
    c->kou_last_send_tick = GetTickCount64();
    c->kou_send_attempts++;
    return 1;
}

static void kou_finish_success(Conn *c, int after_unmute, int response_timeout)
{
    c->kou_pending = 0;
    c->kou_awaiting = 0;
    c->kou_mute_wait = 0;
    c->kou_cooldown_until = time(NULL) + KOU_REPLY_COOLDOWN;

    /* V5 的「扣数字」回复成功后结束本轮 V4 数字配对。 */
    g_follow_active = 0;
    g_follow_deadline = 0;
    memset(g_first_digit, 0, sizeof g_first_digit);

    if (after_unmute) {
        log_port(c->port,
                 "禁言解除后已回复 \"%s\"；V4 全局开关已关闭，进入 1 分钟冷却",
                 c->kou_digit);
    } else if (response_timeout) {
        log_port(c->port,
                 "回复 \"%s\" 2 秒内未收到响应，按 V3 规则视为已发出；V4 全局开关已关闭，进入 1 分钟冷却",
                 c->kou_digit);
    } else {
        log_port(c->port,
                 "Snowluma 确认回复 \"%s\" 成功；V4 全局开关已关闭，进入 1 分钟冷却",
                 c->kou_digit);
    }
}

static void handle_kou_reply_response(Conn *c, const char *json)
{
    char status[24];
    char retbuf[24];
    char wording[256];
    int ok;
    int was_mute_wait = c->kou_mute_wait;

    c->kou_awaiting = 0;
    status[0] = retbuf[0] = '\0';
    json_get_token(json, "status", status, sizeof status);
    json_get_token(json, "retcode", retbuf, sizeof retbuf);

    ok = (strcmp(status, "ok") == 0) || (strcmp(status, "async") == 0)
         || (!status[0] && retbuf[0] && strtoll(retbuf, NULL, 10) == 0);
    if (ok) {
        kou_finish_success(c, was_mute_wait, 0);
        return;
    }

    wording[0] = '\0';
    if (!json_get_token(json, "wording", wording, sizeof wording) || !wording[0])
        json_get_token(json, "message", wording, sizeof wording);

    if (!c->kou_mute_wait) {
        c->kou_mute_wait = 1;
        c->kou_mute_deadline = GetTickCount64()
                               + (unsigned long long)KOU_MUTE_WAIT_SECONDS * 1000ULL;
        log_port(c->port,
                 "回复 \"%s\" 被拒绝（%s）：%s；等待解禁通知，最多 2 分钟",
                 c->kou_digit,
                 strstr(wording, "禁言") ? "账号被禁言" : "疑似被禁言/发送失败",
                 wording[0] ? wording : "原因未知");
    }
}

static void handle_notice(Conn *c, const char *json)
{
    char notice_type[32];
    char group_buf[32];
    char user_buf[64];
    char duration_buf[16];
    int duration;

    if (!json_get_token(json, "notice_type", notice_type, sizeof notice_type)
        || strcmp(notice_type, "group_ban") != 0)
        return;
    if (!json_get_token(json, "group_id", group_buf, sizeof group_buf)
        || strtoll(group_buf, NULL, 10) != GROUP_ID)
        return;
    if (!json_get_token(json, "user_id", user_buf, sizeof user_buf))
        return;

    duration_buf[0] = '\0';
    json_get_token(json, "duration", duration_buf, sizeof duration_buf);
    duration = atoi(duration_buf);

    /* 只处理机器人自身或全员禁言状态。 */
    if (!user_buf[0]
        || (strcmp(user_buf, c->self_id) != 0 && strcmp(user_buf, "0") != 0))
        return;

    if (duration > 0) {
        log_port(c->port, "收到禁言通知：机器人被禁言 %d 秒%s", duration,
                 strcmp(user_buf, "0") == 0 ? "（全员禁言）" : "");
        return;
    }

    log_port(c->port, "收到通知：机器人禁言已解除");
    if (c->kou_pending && c->kou_mute_wait && !c->kou_awaiting
        && c->kou_send_attempts < KOU_MAX_SEND_ATTEMPTS) {
        c->kou_mute_wait = 0;
        if (!kou_reply_send(c))
            conn_drop(c, "解禁后发送回复失败");
    }
}

static void handle_frame(Conn *c, const char *json)
{
    char echo[40];

    if (c->kou_awaiting && json_get_token(json, "echo", echo, sizeof echo)
        && strcmp(echo, c->kou_echo) == 0) {
        handle_kou_reply_response(c, json);
        return;
    }

    if (c->awaiting && json_get_token(json, "echo", echo, sizeof echo)
        && strcmp(echo, c->reply_echo) == 0) {
        handle_reply_response(c, json);
        return;
    }

    /* 只有 3001 会进入群消息判断，其他端口仅处理上面的回复确认。 */
    if (c->port == 3001)
        handle_message(c, json);
}

/* ------------------------------------------------------------------ */
/* 全局开关与数字配对                                                  */
/* ------------------------------------------------------------------ */

static void follow_clear_records(void)
{
    memset(g_first_digit, 0, sizeof g_first_digit);
}

static void follow_expire_if_needed(unsigned long long now)
{
    if (g_follow_active && now >= g_follow_deadline) {
        g_follow_active = 0;
        g_follow_deadline = 0;
        follow_clear_records();
        log_plain("全局开关已到时自动关闭（120 秒），本轮未触发回复");
    }
}

/* 仅检查需要处理的消息；缺少有效 time 字段时保持兼容，不主动丢弃。 */
static int message_is_stale(const char *json)
{
    char buf[32];
    long long message_time;
    long long now;

    if (!json_get_token(json, "time", buf, sizeof buf))
        return 0;
    message_time = strtoll(buf, NULL, 10);
    if (message_time <= 0)
        return 0;
    now = (long long)time(NULL);
    return now > message_time
           && now - message_time > STALE_MESSAGE_SECONDS;
}

static int is_blacklisted(const char *qq)
{
    size_t i;

    for (i = 0; i < sizeof g_blacklist / sizeof g_blacklist[0]; i++)
        if (strcmp(qq, g_blacklist[i]) == 0)
            return 1;
    return 0;
}

/* OneBot 11 的 @全体通常为 [CQ:at,qq=all]，同时兼容附加参数的写法。 */
static int raw_contains_at_all(const char *raw)
{
    return strstr(raw, "[CQ:at,qq=all]") != NULL
           || strstr(raw, "[CQ:at,qq=all,") != NULL;
}

/* V3 的取字方式保留，但「扣」后的字符现在必须是单个 ASCII 数字。 */
static int extract_kou_digit(const char *raw, char *digit)
{
    static const char KOU[] = "\xE6\x89\xA3"; /* 「扣」的 UTF-8 编码 */
    const char *p = raw;

    while ((p = strstr(p, KOU)) != NULL) {
        const char *q = p + 3;

        while (*q == ' ' || *q == '\t'
               || ((unsigned char)q[0] == 0xE3
                   && q[1] != '\0' && q[2] != '\0'
                   && (unsigned char)q[1] == 0x80
                   && (unsigned char)q[2] == 0x80))
            q += (*q == ' ' || *q == '\t') ? 1 : 3;

        if (*q >= '0' && *q <= '9') {
            *digit = *q;
            return 1;
        }
        p += 3;
    }
    return 0;
}

static void handle_message(Conn *c, const char *json)
{
    char post_type[32];
    char message_type[32];
    char group_buf[32];
    char user_buf[64];
    char raw[4096];
    int target_image;
    int target_at_all;
    int target_trigger;
    int blacklisted;
    int one_digit;
    char kou_digit = '\0';
    int has_kou_digit = 0;
    const char *trigger_type;
    unsigned long long tick;

    if (!json_get_token(json, "post_type", post_type, sizeof post_type))
        return;

    /* self_id 也可能随通知事件提供，供 group_ban 解禁判断使用。 */
    {
        char sid[32];

        if (json_get_token(json, "self_id", sid, sizeof sid))
            snprintf(c->self_id, sizeof c->self_id, "%s", sid);
    }
    if (strcmp(post_type, "notice") == 0) {
        handle_notice(c, json);
        return;
    }
    if (strcmp(post_type, "message") != 0)
        return;
    if (!json_get_token(json, "message_type", message_type, sizeof message_type)
        || strcmp(message_type, "group") != 0)
        return;
    if (!json_get_token(json, "group_id", group_buf, sizeof group_buf)
        || strtoll(group_buf, NULL, 10) != GROUP_ID)
        return;
    if (!json_get_token(json, "raw_message", raw, sizeof raw)
        || !json_get_token(json, "user_id", user_buf, sizeof user_buf))
        return;

    /* 自己的数字回复回显不参与配对。 */
    if (c->self_id[0] && strcmp(user_buf, c->self_id) == 0)
        return;

    blacklisted = is_blacklisted(user_buf);
    target_image = strcmp(user_buf, TARGET_QQ) == 0
                   && strstr(raw, "[CQ:image") != NULL;
    target_at_all = strcmp(user_buf, TARGET_QQ) == 0
                    && raw_contains_at_all(raw);
    target_trigger = target_image || target_at_all;
    if (target_at_all)
        has_kou_digit = extract_kou_digit(raw, &kou_digit);
    trigger_type = target_image && target_at_all ? "图片+@全体"
                   : target_image ? "图片" : "@全体";
    one_digit = raw[0] >= '0' && raw[0] <= '9' && raw[1] == '\0';

    /* 黑名单任意消息都可能关闭开关；其他无关消息直接忽略。 */
    if (!blacklisted && !target_trigger && !one_digit)
        return;

    /* 程序若因外部原因暂停，恢复后不使用积压的相关消息补触发。 */
    if (message_is_stale(json)) {
        log_port(3001, "丢弃用户 %s 的延迟消息（超过 %d 秒），不用于开关控制或配对",
                 user_buf, STALE_MESSAGE_SECONDS);
        return;
    }

    tick = GetTickCount64();
    follow_expire_if_needed(tick);

    if (blacklisted) {
        if (g_follow_active) {
            g_follow_active = 0;
            g_follow_deadline = 0;
            follow_clear_records();
            log_port(3001,
                     "检测到黑名单用户 %s 发送群消息，全局开关已立即关闭，配对表已清空",
                     user_buf);
        }
        return;
    }

    if (target_trigger) {
        /* 图片和 @全体使用同一套开关；最后一条触发消息开启新一轮配对。 */
        follow_clear_records();
        g_follow_deadline = tick + FOLLOW_SWITCH_SECONDS * 1000ULL;
        if (g_follow_active) {
            log_port(3001,
                     "目标 QQ %s 再次发送%s，已清空上次配对表，并以本条消息重新计时 %d 秒",
                     TARGET_QQ, trigger_type, FOLLOW_SWITCH_SECONDS);
        } else {
            g_follow_active = 1;
            log_port(3001,
                     "目标 QQ %s 发送%s，全局开关已开启（%d 秒），开始记录一位纯数字",
                     TARGET_QQ, trigger_type, FOLLOW_SWITCH_SECONDS);
        }

        /* @全体+扣数字优先执行 V3 回复；图片单独触发时仍只走 V4 开关。 */
        if (target_at_all && has_kou_digit) {
            if (c->kou_pending) {
                log_port(3001,
                         "目标 QQ 的 @全体 消息含 \"扣%c\"，已有 V3 回复/解禁等待，本次不重复发送",
                         kou_digit);
            } else if (c->kou_cooldown_until > time(NULL)) {
                log_port(3001,
                         "目标 QQ 的 @全体 消息含 \"扣%c\"，V3 冷却中（剩余 %lld 秒），不回复",
                         kou_digit,
                         (long long)(c->kou_cooldown_until - time(NULL)));
            } else {
                snprintf(c->kou_user, sizeof c->kou_user, "%s", user_buf);
                snprintf(c->kou_digit, sizeof c->kou_digit, "%c", kou_digit);
                c->kou_pending = 1;
                c->kou_mute_wait = 0;
                c->kou_send_attempts = 0;
                if (!kou_reply_send(c)) {
                    c->kou_pending = 0;
                    log_port(3001, "V3 扣数字回复发送失败，连接可能已断开");
                } else {
                    log_port(3001,
                             "目标 QQ 的 @全体 消息含 \"扣%c\"，已按 V3 回复 \"%c\"；等待成功确认，期间暂停数字配对",
                             kou_digit, kou_digit);
                }
            }
        }
        return;
    }

    /* V3 回复尚在等确认或解禁通知时，避免同时触发 V4 数字配对。 */
    if (c->kou_pending) {
        if (one_digit)
            log_port(3001, "V3 扣数字回复处理中，暂不记录新的配对数字");
        return;
    }

    if (!g_follow_active) {
        log_port(3001, "用户 %s 发送数字 %c，但全局开关关闭，忽略且不回复",
                 user_buf, raw[0]);
        return;
    }

    {
        int index = raw[0] - '0';
        FirstDigit *first = &g_first_digit[index];

        /* 首条记录超过 30 秒即失效；当前消息重新成为首位记录，不与旧消息配对。 */
        if (first->used
            && tick - first->tick > FOLLOW_MATCH_SECONDS * 1000ULL) {
            log_port(3001,
                     "用户 %s 的数字 %c 首位记录已超过 %d 秒，旧记录作废",
                     first->qq, raw[0], FOLLOW_MATCH_SECONDS);
            first->used = 0;
        }

        if (!first->used) {
            first->used = 1;
            first->tick = tick;
            snprintf(first->qq, sizeof first->qq, "%s", user_buf);
            log_port(3001,
                     "记录数字 %c 的首位用户 %s，等待其他用户在 %d 秒内发送相同数字",
                     raw[0], user_buf, FOLLOW_MATCH_SECONDS);
            return;
        }

        if (strcmp(first->qq, user_buf) == 0) {
            log_port(3001,
                     "用户 %s 重复发送数字 %c，同一用户重复发送不计入配对，也不延长原记录的 30 秒窗口",
                     user_buf, raw[0]);
            return;
        }

        /* 先在发送前关闭开关并清空本轮记录，杜绝任何重复触发。 */
        {
            char first_qq[64];
            char digit = raw[0];

            snprintf(first_qq, sizeof first_qq, "%s", first->qq);
            g_follow_active = 0;
            g_follow_deadline = 0;
            follow_clear_records();

            /* 不在三次发送之间等待响应；开关关闭后也不会再处理新的配对。 */
            reply_submit_all(digit);
            log_port(3001,
                     "用户 %s 与用户 %s 的数字 %c 在 30 秒内首先配对成功；全局开关已关闭",
                     user_buf, first_qq, digit);
        }
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
    c->connected = 0;
    c->len = 0;
    c->awaiting = 0;
    c->kou_awaiting = 0;
    c->kou_pending = 0;
    c->kou_mute_wait = 0;
    c->next_try = GetTickCount64() + RECONNECT_DELAY * 1000ULL;
    if (reason)
        log_port(c->port, "%s，%d 秒后自动重连", reason, RECONNECT_DELAY);
}

static int conn_try_connect(Conn *c)
{
    SOCKET s;
    char reason[256];

    log_port(c->port, "正在连接 ws://%s:%d ...", HOST, c->port);
    s = tcp_connect(HOST, c->port, CONNECT_TIMEOUT_MS);
    if (s == INVALID_SOCKET) {
        c->next_try = GetTickCount64() + RECONNECT_DELAY * 1000ULL;
        log_port(c->port, "连接失败，%d 秒后自动重连", RECONNECT_DELAY);
        return 0;
    }

    if (!ws_handshake(s, c->port, reason, sizeof reason)) {
        closesocket(s);
        c->next_try = GetTickCount64() + RECONNECT_DELAY * 1000ULL;
        log_port(c->port, "%s，%d 秒后自动重连", reason, RECONNECT_DELAY);
        return 0;
    }

    c->sock = s;
    c->connected = 1;
    c->len = 0;
    c->awaiting = 0;
    if (c->port == 3001)
        log_port(c->port, "WebSocket 已连接；负责接收群消息、开关计时和数字配对");
    else
        log_port(c->port, "WebSocket 已连接；仅用于配对成功后的群内回复");
    return 1;
}

static void conn_parse_frames(Conn *c)
{
    while (c->len >= 2) {
        unsigned char *b = (unsigned char *)c->buf;
        int opcode = b[0] & 0x0F;
        int masked = (b[1] & 0x80) != 0;
        unsigned long long plen = b[1] & 0x7F;
        size_t hdr = 2;
        size_t i;

        if (plen == 126) {
            if (c->len < 4)
                return;
            plen = ((unsigned long long)b[2] << 8) | b[3];
            hdr = 4;
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

        if (plen > (unsigned long long)(sizeof c->buf - hdr)) {
            conn_drop(c, "收到异常超长 WebSocket 帧");
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
                /* 3001 回复发送失败时可能已在 handle_frame 中断线并重置缓冲区。 */
                if (!c->connected)
                    return;
            }
        } else if (opcode == 0x8) {
            conn_drop(c, "Snowluma 关闭了 WebSocket");
            return;
        } else if (opcode == 0x9) {
            /* ping -> pong；按 WebSocket 协议原样带回 ping 载荷。 */
            unsigned char pong[131];
            unsigned char mk[4];

            if (plen > 125) {
                conn_drop(c, "收到非法长度的 ping 帧");
                return;
            }
            pong[0] = 0x8A;
            pong[1] = (unsigned char)(0x80 | plen);
            random_bytes(mk, sizeof mk);
            memcpy(pong + 2, mk, sizeof mk);
            for (i = 0; i < (size_t)plen; i++)
                pong[6 + i] = b[hdr + i] ^ mk[i & 3];
            if (!send_all(c->sock, (const char *)pong, 6 + (size_t)plen)) {
                conn_drop(c, "发送 pong 时连接失败");
                return;
            }
        }

        memmove(c->buf, c->buf + hdr + (size_t)plen,
                c->len - hdr - (size_t)plen);
        c->len -= hdr + (size_t)plen;
    }
}

static void conn_read(Conn *c)
{
    int n;

    if (c->len >= sizeof c->buf) {
        conn_drop(c, "接收缓冲区已满");
        return;
    }

    n = recv(c->sock, c->buf + c->len,
             (int)(sizeof c->buf - c->len), 0);
    if (n == 0) {
        conn_drop(c, "连接被对端关闭");
        return;
    }
    if (n < 0) {
        int e = WSAGetLastError();

        if (e == WSAEWOULDBLOCK)
            return;
        conn_drop(c, "读取消息时连接失败");
        return;
    }

    c->len += (size_t)n;
    conn_parse_frames(c);
}

/* ------------------------------------------------------------------ */
/* 启动端口检测                                                        */
/* ------------------------------------------------------------------ */

static void scan_ports(void)
{
    int i;

    memset(g_conns, 0, sizeof g_conns);
    log_plain("开始检测启动端口可用性：3001 / 3002 / 3003");

    for (i = 0; i < PORT_COUNT; i++) {
        Conn *c = &g_conns[i];
        int ok;

        c->port = g_ports[i];
        c->sock = INVALID_SOCKET;
        c->next_try = 0;
        ok = probe_port(c->port);

        if (c->port == 3001) {
            /* 主端口无论启动是否可用，都必须永久尝试连接。 */
            c->enabled = 1;
            if (ok)
                log_port(c->port, "启动检测可用，将连接并持续监听群消息");
            else
                log_port(c->port,
                         "启动检测不可用；主端口必须运行，将每 %d 秒无限重连",
                         RECONNECT_DELAY);
        } else if (ok) {
            c->enabled = 1;
            log_port(c->port, "启动检测可用，将用于回复；中途断线会自动重连");
        } else {
            c->enabled = 0;
            log_port(c->port,
                     "启动检测不可用；本次运行不监听、不重连，触发时也不回复");
        }
    }
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

int main(void)
{
    WSADATA wsa;
    unsigned seed;
    unsigned long long next_console_check;
    int i;

    out_init();
    log_init();
    SetConsoleCtrlHandler(ctrl_handler, TRUE);
    disable_console_quick_edit(1);
    setvbuf(stdout, NULL, _IONBF, 0);

    seed = (unsigned)GetTickCount64() ^ (unsigned)GetCurrentProcessId();
    srand(seed);

    out_text("============================================================\n");
    out_text("  Snowluma V5 三端口数字跟发与扣数字回复工具\n");
    out_text("============================================================\n");
    out_text("  3001：接收消息、图片/@全体开关、数字配对并参与回复\n");
    out_text("  3002/3003：仅参与配对成功后的回复\n");
    out_fmt("  群号：%s；目标 QQ：%s\n", GROUP_ID_STR, TARGET_QQ);
    out_text("  图片/@全体开关：120 秒；最后一条触发消息清表并重置计时\n");
    out_text("  数字规则：一位纯数字 0~9；不同用户 30 秒内相同数字才配对\n");
    out_text("  黑名单：3117669520 / 2248908690 / 202945218\n");
    out_fmt("  防补触发：超过 %d 秒的相关旧消息直接丢弃\n",
            STALE_MESSAGE_SECONDS);
    out_fmt("  回复确认：%d 秒内未确认成功则放弃，不重试、不补发\n",
            REPLY_RESPONSE_TIMEOUT / 1000);
    out_text("  防卡顿：快速编辑禁用并定期复查；异步有界日志队列\n");
    out_text("  @全体消息含扣+一位数字时由 3001 回复；成功后关闭开关并冷却 60 秒\n");
    out_text("  扣数字回复被拒时等待解禁通知（最多 2 分钟），解禁后立即重发\n");
    out_text("============================================================\n\n");

    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        log_plain("WSAStartup 失败，无法初始化网络");
        log_shutdown();
        return 1;
    }

    scan_ports();
    log_plain("程序运行中，按 Ctrl+C 退出；3001 未连接时会持续重连\n");
    next_console_check = GetTickCount64() + CONSOLE_RECHECK_MS;

    while (g_running) {
        unsigned long long now = GetTickCount64();
        fd_set rf;
        struct timeval tv;
        int any = 0;
        int r;

        follow_expire_if_needed(now);
        if (now >= next_console_check) {
            disable_console_quick_edit(0);
            next_console_check = now + CONSOLE_RECHECK_MS;
        }

        /* 所有 enabled 端口断线后均自动重连；3001 启动不可用也会一直尝试。 */
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (c->enabled && !c->connected && now >= c->next_try)
                conn_try_connect(c);
        }

        /* V3 扣数字回复：等待结果、解禁通知或 V3 的 2 秒响应兜底。 */
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (!c->enabled || !c->connected || !c->kou_pending)
                continue;

            if (c->kou_mute_wait) {
                if (now >= c->kou_mute_deadline) {
                    c->kou_mute_wait = 0;
                    c->kou_pending = 0;
                    c->kou_awaiting = 0;
                    log_port(c->port,
                             "2 分钟内未收到解除禁言通知，放弃本次扣数字回复（不进入冷却）");
                }
            } else if (now - c->kou_last_send_tick
                       >= KOU_REPLY_RESPONSE_TIMEOUT) {
                /* V3 语义：响应超过 2 秒未到时按已发出处理并进入冷却。 */
                kou_finish_success(c, 0, 1);
            }
        }

        /* V4 数字配对回复确认只影响日志，不重试、不补发。 */
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (c->connected && c->awaiting
                && now - c->last_send_tick >= REPLY_RESPONSE_TIMEOUT) {
                c->awaiting = 0;
                log_port(c->port,
                         "数字 %s 的回复确认超时；不等待、不重试、不补发",
                         c->last_digit[0] ? c->last_digit : "?");
            }
        }

        FD_ZERO(&rf);
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (c->enabled && c->connected) {
                FD_SET(c->sock, &rf);
                any = 1;
            }
        }

        if (!any) {
            Sleep(SELECT_TIMEOUT_MS);
            continue;
        }

        tv.tv_sec = 0;
        tv.tv_usec = SELECT_TIMEOUT_MS * 1000;
        r = select(0, &rf, NULL, NULL, &tv);
        if (r <= 0)
            continue;

        /* 3001 在数组首位，事件到达后优先处理，配对成功立即提交三端回复。 */
        for (i = 0; i < PORT_COUNT; i++) {
            Conn *c = &g_conns[i];

            if (c->enabled && c->connected && FD_ISSET(c->sock, &rf))
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
    log_plain("程序已退出");
    log_shutdown();
    return 0;
}
