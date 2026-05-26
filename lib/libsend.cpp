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

/* Saved values from init_sender. Kept for completeness. */
static int g_speed = 8;
static int g_delay = 2;

/* Maximum number of segments inflight. 16 * 512 = 8 KB which fits in the
 * 9 KB receive buffer used by the server. */
static const int MAX_INFLIGHT = 16;

/* Retransmit a segment that has not been ACKed for too long. */
static const long RETX_THRESHOLD_MS = 30;

/* Send one segment over UDP. */
static void send_segment(struct connection *con, const sent_segment &seg)
{
    sendto(con->sockfd, seg.data, seg.len, 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

/* Milliseconds between two timespecs */
static long elapsed_ms(const struct timespec &a, const struct timespec &b)
{
    return (b.tv_sec - a.tv_sec) * 1000 + (b.tv_nsec - a.tv_nsec) / 1000000;
}

int send_data(int conn_id, char *buffer, int len)
{
    int sent = 0;
    struct connection *con = cons[conn_id];

    /* Keep pushing segments until everything fits in the window. We do this
     * because client.cpp sleeps 500 ms whenever send_data returns less than
     * the requested amount, which kills throughput. */
    while (sent < len) {
        pthread_mutex_lock(&con->con_lock);

        while ((int)con->unacked.size() < MAX_INFLIGHT && sent < len) {
            int chunk = std::min((int)MAX_DATA_SIZE, len - sent);

            sent_segment seg;
            poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)seg.data;
            hdr->protocol_id = POLI_PROTOCOL_ID;
            hdr->conn_id = (uint8_t)con->conn_id;
            hdr->type = POLI_TYPE_DATA;
            hdr->seq_num = htons(con->next_seq);
            hdr->len = htons((uint16_t)chunk);
            memcpy(seg.data + sizeof(*hdr), buffer + sent, chunk);
            seg.len = sizeof(*hdr) + chunk;
            seg.seq = con->next_seq;

            clock_gettime(CLOCK_MONOTONIC, &seg.send_time);
            send_segment(con, seg);
            con->unacked.push_back(seg);
            con->next_seq++;
            sent += chunk;
        }

        pthread_mutex_unlock(&con->con_lock);

        if (sent < len) usleep(100); /* wait for ACKs to free the window */
    }

    return sent;
}

void *sender_handler(void *arg)
{
    char buf[MAX_SEGMENT_SIZE];
    int res = 0;

    while (1) {

        if (cons.size() == 0) {
            continue;
        }

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        if (cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];

        if (res >= (int)sizeof(poli_tcp_ctrl_hdr)) {
            /* Got something from the receiver. We only care about ACKs. */
            poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;
            if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == POLI_TYPE_ACK) {
                uint16_t ack = ntohs(hdr->ack_num);
                con->peer_window = ntohs(hdr->recv_window);

                /* Cumulative ACK: drop everything below ack */
                while (!con->unacked.empty() && con->unacked.front().seq < ack) {
                    con->unacked.pop_front();
                }
                con->base_seq = ack;
            }
        } else if (res == -1) {
            /* Timer expired: only retransmit segments that have been waiting
             * for an ACK longer than the threshold. This avoids resending
             * packets that are still legitimately in flight. */
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            for (auto &seg : con->unacked) {
                if (elapsed_ms(seg.send_time, now) >= RETX_THRESHOLD_MS) {
                    send_segment(con, seg);
                    seg.send_time = now;
                }
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    struct connection *con = new struct connection();
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = port;
    con->conn_id = 0;
    con->next_seq = 0;
    con->base_seq = 0;
    con->peer_window = 9 * 1024;
    pthread_mutex_init(&con->con_lock, NULL);

    /* 3-way handshake. Use a short recv timeout so we can retransmit
     * the SYN if SYN or SYN-ACK gets lost. */
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        /* Send SYN */
        poli_tcp_ctrl_hdr syn;
        syn.protocol_id = POLI_PROTOCOL_ID;
        syn.conn_id = 0;
        syn.type = POLI_TYPE_SYN;
        syn.ack_num = 0;
        syn.recv_window = htons(9 * 1024);
        sendto(con->sockfd, &syn, sizeof(syn), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        /* Wait for SYN-ACK */
        int n = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, NULL, NULL);
        if (n < (int)(sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t))) {
            continue;
        }
        poli_tcp_ctrl_hdr *r = (poli_tcp_ctrl_hdr *)buf;
        if (r->protocol_id != POLI_PROTOCOL_ID || r->type != POLI_TYPE_SYNACK) {
            continue;
        }

        /* Server told us which port to use for this connection. */
        uint16_t new_port;
        memcpy(&new_port, buf + sizeof(poli_tcp_ctrl_hdr), sizeof(new_port));
        con->servaddr.sin_port = new_port;
        con->conn_id = r->conn_id;

        /* Send the final ACK */
        poli_tcp_ctrl_hdr ack;
        ack.protocol_id = POLI_PROTOCOL_ID;
        ack.conn_id = (uint8_t)con->conn_id;
        ack.type = POLI_TYPE_ACK;
        ack.ack_num = 0;
        ack.recv_window = htons(9 * 1024);
        sendto(con->sockfd, &ack, sizeof(ack), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
        break;
    }

    /* Disable the receive timeout for normal operation */
    struct timeval tv0 = {0, 0};
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv0, sizeof(tv0));

    cons.insert({con->conn_id, con});

    /* Register socket for the handler thread to poll. */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* Per-connection timer used to drive retransmissions. Fires every 10 ms;
     * the handler decides per-segment which ones are actually due. */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 10000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 10000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return con->conn_id;
}

void init_sender(int speed, int delay)
{
    g_speed = speed;
    g_delay = delay;

    pthread_t thread1;
    int ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
