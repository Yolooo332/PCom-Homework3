#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

static int retx_timeout_ms = 30;

#define WINDOW_SEGMENTS 16

int send_data(int conn_id, char *buffer, int len)
{
    struct connection *con = cons[conn_id];
    int sent = 0;

    while (sent < len)
    {
        pthread_mutex_lock(&con->con_lock);

        /* We will write code here as to not have sync problems with sender_handler */
        while ((int)con->unacked.size() < WINDOW_SEGMENTS && sent < len)
        {
            int chunk = min((int)MAX_DATA_SIZE, len - sent);

            poli_tcp_data_hdr hdr;
            hdr.protocol_id = POLI_PROTOCOL_ID;
            hdr.conn_id = (uint8_t)con->conn_id;
            hdr.type = POLI_TYPE_DATA;
            hdr.seq_num = htons(con->next_seq);
            hdr.len = htons((uint16_t)chunk);

            sent_segment seg;
            memcpy(seg.data, &hdr, sizeof(hdr));
            memcpy(seg.data + sizeof(hdr), buffer + sent, chunk);
            seg.len = sizeof(hdr) + chunk;
            seg.seq = con->next_seq;
            clock_gettime(CLOCK_MONOTONIC, &seg.send_time);

            sendto(con->sockfd, seg.data, seg.len, 0,
                   (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

            con->unacked.push_back(seg);
            con->next_seq++;
            sent += chunk;
        }

        pthread_mutex_unlock(&con->con_lock);

        if (sent < len)
            usleep(100);
    }

    return sent;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1)
    {

        if (cons.size() == 0)
        {
            continue;
        }
        int conn_id = -1;
        do
        {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        if (cons.find(conn_id) == cons.end())
            continue;

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        if (res >= (int)sizeof(poli_tcp_ctrl_hdr))
        {
            poli_tcp_ctrl_hdr *ctrl = (poli_tcp_ctrl_hdr *)buf;

            if (ctrl->protocol_id == POLI_PROTOCOL_ID && ctrl->type == POLI_TYPE_ACK)
            {
                uint16_t ack = ntohs(ctrl->ack_num);

                while (!con->unacked.empty() && con->unacked.front().seq < ack)
                    con->unacked.pop_front();

                con->base_seq = ack;
                con->peer_window = ntohs(ctrl->recv_window);
            }
        }
        else if (res == -1)
        {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);

            for (size_t i = 0; i < con->unacked.size(); i++)
            {
                sent_segment &seg = con->unacked[i];
                long age = (now.tv_sec - seg.send_time.tv_sec) * 1000 + (now.tv_nsec - seg.send_time.tv_nsec) / 1000000;
                if (age >= retx_timeout_ms)
                {
                    sendto(con->sockfd, seg.data, seg.len, 0,
                           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
                    seg.send_time = now;
                }
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

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

    /* // This can be used to set a timer on a socket
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 300000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    {
        perror("Error");
    }

    char buf[MAX_SEGMENT_SIZE];

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */
    while (1)
    {
        poli_tcp_ctrl_hdr syn;
        syn.protocol_id = POLI_PROTOCOL_ID;
        syn.conn_id = 0;
        syn.type = POLI_TYPE_SYN;
        syn.ack_num = 0;
        syn.recv_window = htons(9 * 1024);
        sendto(con->sockfd, &syn, sizeof(syn), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        int n = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, NULL, NULL);
        if (n < (int)(sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)))
            continue;

        poli_tcp_ctrl_hdr *r = (poli_tcp_ctrl_hdr *)buf;
        if (r->protocol_id != POLI_PROTOCOL_ID || r->type != POLI_TYPE_SYNACK)
            continue;

        uint16_t data_port;
        memcpy(&data_port, buf + sizeof(poli_tcp_ctrl_hdr), sizeof(data_port));
        con->servaddr.sin_port = data_port;
        con->conn_id = r->conn_id;

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

    struct timeval off = {0, 0};
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &off, sizeof(off));

    cons.insert({con->conn_id, con});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 10 * 1000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 10 * 1000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return con->conn_id;
}

void init_sender(int speed, int delay)
{
    retx_timeout_ms = TIMEOUT_SEND(delay);

    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
