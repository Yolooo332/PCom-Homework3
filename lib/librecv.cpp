#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <map>
#include <cstdint>
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <algorithm>
#include "lib.h"
#include "utils.h"
#include "protocol.h"

using namespace std;

/* Per-binary globals (also referenced as extern from libcommon.cpp). */
std::map<int, struct connection *> cons;
struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Listening socket on port 8032. Bound once on the first wait4connect call. */
static int accept_sockfd = -1;
/* Connection id we assign to the next accepted client. */
static int next_conn_id = 0;
/* Saved from init_receiver, used as advertised window for flow control. */
static int g_recv_buf_bytes = 9 * 1024;

/* Build and send an ACK segment to the peer of this connection. */
static void send_ack(struct connection *con, uint16_t ack_num)
{
    poli_tcp_ctrl_hdr hdr;
    hdr.protocol_id = POLI_PROTOCOL_ID;
    hdr.conn_id = (uint8_t)con->conn_id;
    hdr.type = POLI_TYPE_ACK;
    hdr.ack_num = htons(ack_num);

    int free_window = con->max_recv_buf - (int)con->recv_buf.size();
    if (free_window < 0) free_window = 0;
    hdr.recv_window = htons((uint16_t)free_window);

    sendto(con->sockfd, &hdr, sizeof(hdr), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

int recv_data(int conn_id, char *buffer, int len)
{
    /* Block until there is data available in the connection's buffer. */
    while (1) {
        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];

        if (!con->recv_buf.empty()) {
            int n = std::min((int)con->recv_buf.size(), len);
            for (int i = 0; i < n; i++) {
                buffer[i] = con->recv_buf.front();
                con->recv_buf.pop_front();
            }
            pthread_mutex_unlock(&con->con_lock);
            return n;
        }
        pthread_mutex_unlock(&con->con_lock);
        usleep(1000);
    }
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        if (cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];

        if (res >= (int)sizeof(poli_tcp_data_hdr)) {
            poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;
            if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == POLI_TYPE_DATA) {
                uint16_t seq = ntohs(hdr->seq_num);
                uint16_t plen = ntohs(hdr->len);
                const char *payload = segment + sizeof(poli_tcp_data_hdr);

                if (seq == con->expected_seq) {
                    /* In order: deliver and flush buffered out-of-order segments */
                    con->recv_buf.insert(con->recv_buf.end(), payload, payload + plen);
                    con->expected_seq++;
                    while (con->out_of_order.count(con->expected_seq)) {
                        auto &v = con->out_of_order[con->expected_seq];
                        con->recv_buf.insert(con->recv_buf.end(), v.begin(), v.end());
                        con->out_of_order.erase(con->expected_seq);
                        con->expected_seq++;
                    }
                } else if (seq > con->expected_seq) {
                    /* Out of order: buffer it */
                    con->out_of_order[seq] = std::vector<char>(payload, payload + plen);
                }
                /* else: duplicate, drop */

                /* Always reply with the current expected_seq as cumulative ACK */
                send_ack(con, con->expected_seq);
            }
        }
        /* res == -1: timer fired. Nothing to do on receiver side. */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
    return NULL;
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* Lazily bind the accept socket the first time someone calls wait4connect. */
    if (accept_sockfd == -1) {
        accept_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        int reuse = 1;
        setsockopt(accept_sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ip;
        addr.sin_port = port;
        int rc = bind(accept_sockfd, (struct sockaddr *)&addr, sizeof(addr));
        assert(rc == 0);
    }

    struct connection *con = new struct connection();
    con->conn_id = next_conn_id++;
    con->expected_seq = 0;
    con->max_recv_buf = g_recv_buf_bytes;
    pthread_mutex_init(&con->con_lock, NULL);

    char buf[MAX_SEGMENT_SIZE];
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    /* Wait for a SYN on the accept socket. If we see a SYN from a client we
     * already accepted earlier, it's a retransmit (its SYN-ACK got lost).
     * Resend the SYN-ACK from the existing per-client socket and keep waiting
     * for a brand new client. */
    while (1) {
        int n = recvfrom(accept_sockfd, buf, MAX_SEGMENT_SIZE, 0,
                         (struct sockaddr *)&client_addr, &client_len);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr)) continue;
        poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;
        if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != POLI_TYPE_SYN) continue;

        bool duplicate = false;
        for (auto &kv : cons) {
            struct connection *ex = kv.second;
            if (ex->servaddr.sin_addr.s_addr == client_addr.sin_addr.s_addr &&
                ex->servaddr.sin_port == client_addr.sin_port) {
                struct sockaddr_in ex_bind;
                socklen_t sl = sizeof(ex_bind);
                getsockname(ex->sockfd, (struct sockaddr *)&ex_bind, &sl);
                struct {
                    poli_tcp_ctrl_hdr hdr;
                    uint16_t port;
                } __attribute__((packed)) synack;
                synack.hdr.protocol_id = POLI_PROTOCOL_ID;
                synack.hdr.conn_id = (uint8_t)ex->conn_id;
                synack.hdr.type = POLI_TYPE_SYNACK;
                synack.hdr.ack_num = 0;
                synack.hdr.recv_window = htons((uint16_t)ex->max_recv_buf);
                synack.port = ex_bind.sin_port;
                sendto(ex->sockfd, &synack, sizeof(synack), 0,
                       (struct sockaddr *)&ex->servaddr, sizeof(ex->servaddr));
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        break;
    }

    /* Open a fresh socket on a random local port for the rest of the connection. */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in con_bind;
    memset(&con_bind, 0, sizeof(con_bind));
    con_bind.sin_family = AF_INET;
    con_bind.sin_addr.s_addr = INADDR_ANY;
    con_bind.sin_port = 0;
    int rc = bind(con->sockfd, (struct sockaddr *)&con_bind, sizeof(con_bind));
    assert(rc == 0);

    socklen_t sl = sizeof(con_bind);
    getsockname(con->sockfd, (struct sockaddr *)&con_bind, &sl);
    uint16_t chosen_port = con_bind.sin_port; /* network byte order */

    con->servaddr = client_addr;

    /* Short timeout so we can retransmit SYN-ACK if the client's ACK gets lost. */
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    while (1) {
        /* Send SYN-ACK with the chosen port as payload */
        struct {
            poli_tcp_ctrl_hdr hdr;
            uint16_t port;
        } __attribute__((packed)) synack;
        synack.hdr.protocol_id = POLI_PROTOCOL_ID;
        synack.hdr.conn_id = (uint8_t)con->conn_id;
        synack.hdr.type = POLI_TYPE_SYNACK;
        synack.hdr.ack_num = 0;
        synack.hdr.recv_window = htons((uint16_t)con->max_recv_buf);
        synack.port = chosen_port;
        sendto(con->sockfd, &synack, sizeof(synack), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        /* Wait for final ACK on the new socket */
        int n = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, NULL, NULL);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr)) continue;
        poli_tcp_ctrl_hdr *r = (poli_tcp_ctrl_hdr *)buf;
        if (r->protocol_id != POLI_PROTOCOL_ID) continue;
        /* Accept either an explicit ACK, or DATA (means our ACK got through but
         * the client moved on and we lost the ACK ourselves). */
        if (r->type == POLI_TYPE_ACK || r->type == POLI_TYPE_DATA) break;
    }

    /* Clear timeout for normal data flow */
    struct timeval tv0 = {0, 0};
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv0, sizeof(tv0));

    cons.insert({con->conn_id, con});

    /* Register socket and timer for the handler thread. */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 1;
    spec.it_value.tv_nsec = 0;
    spec.it_interval.tv_sec = 1;
    spec.it_interval.tv_nsec = 0;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return con->conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    g_recv_buf_bytes = recv_buffer_bytes;

    pthread_t thread1;
    int ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
