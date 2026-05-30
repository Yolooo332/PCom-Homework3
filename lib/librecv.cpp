#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <algorithm>

using namespace std;

/* Intentionally leaked (never deleted): the handler thread loops forever over
   `cons`, so the map must outlive main(). A plain global would have its
   destructor run at process exit while that thread is still iterating it -> a
   use-after-free on shutdown. Leaking the map removes that teardown race. */
std::map<int, struct connection *> &cons = *new std::map<int, struct connection *>();

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

static int listen_sock = -1;
static int conn_counter = 0;
static int recv_window_bytes = 9 * 1024;

static void send_ack(struct connection *con, uint16_t ack_num)
{
    poli_tcp_ctrl_hdr ack;
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = (uint8_t)con->conn_id;
    ack.type = POLI_TYPE_ACK;
    ack.ack_num = htons(ack_num);

    int room = con->max_recv_buf - (int)con->recv_buf.size();
    if (room < 0)
        room = 0;
    ack.recv_window = htons((uint16_t)room);

    char pkt[sizeof(ack) + POLI_CSUM_SIZE];
    memcpy(pkt, &ack, sizeof(ack));
    sendto(con->sockfd, pkt, poli_seal(pkt, sizeof(ack)), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

static void send_synack(int sockfd, struct sockaddr_in *dst, int conn_id,
                        int window, uint16_t data_port)
{
    struct
    {
        poli_tcp_ctrl_hdr hdr;
        uint16_t port;
    } __attribute__((packed)) msg;

    msg.hdr.protocol_id = POLI_PROTOCOL_ID;
    msg.hdr.conn_id = (uint8_t)conn_id;
    msg.hdr.type = POLI_TYPE_SYNACK;
    msg.hdr.ack_num = 0;
    msg.hdr.recv_window = htons((uint16_t)window);
    msg.port = data_port;

    char pkt[sizeof(msg) + POLI_CSUM_SIZE];
    memcpy(pkt, &msg, sizeof(msg));
    sendto(sockfd, pkt, poli_seal(pkt, sizeof(msg)), 0, (struct sockaddr *)dst, sizeof(*dst));
}

int recv_data(int conn_id, char *buffer, int len)
{
    struct connection *con = cons[conn_id];

    while (1)
    {
        pthread_mutex_lock(&con->con_lock);

        /* We will write code here as to not have sync problems with recv_handler */
        if (!con->recv_buf.empty())
        {
            int n = min((int)con->recv_buf.size(), len);
            for (int i = 0; i < n; i++)
            {
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

    while (1)
    {

        int conn_id = -1;
        do
        {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        pthread_mutex_lock(&registry_lock);
        auto it_con = cons.find(conn_id);
        struct connection *con = (it_con == cons.end()) ? NULL : it_con->second;
        pthread_mutex_unlock(&registry_lock);
        if (con == NULL)
            continue;

        pthread_mutex_lock(&con->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */
        int payload_len = poli_verify(segment, res);
        if (payload_len >= (int)sizeof(poli_tcp_data_hdr))
        {
            poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;

            if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == POLI_TYPE_DATA)
            {
                uint16_t seq = ntohs(hdr->seq_num);
                uint16_t paylen = ntohs(hdr->len);
                char *payload = segment + sizeof(poli_tcp_data_hdr);

                /* Never trust the header length past the bytes we actually got
                   (the CRC normally rejects a mangled len, but guard the copy so
                   a freak collision can't over-read past the segment buffer). */
                int avail = payload_len - (int)sizeof(poli_tcp_data_hdr);
                if ((int)paylen > avail)
                    paylen = (uint16_t)avail;

                if (seq == con->expected_seq)
                {
                    con->recv_buf.insert(con->recv_buf.end(), payload, payload + paylen);
                    con->expected_seq++;

                    map<uint16_t, vector<char>>::iterator it;
                    while ((it = con->out_of_order.find(con->expected_seq)) !=
                           con->out_of_order.end())
                    {
                        con->recv_buf.insert(con->recv_buf.end(),
                                             it->second.begin(), it->second.end());
                        con->out_of_order.erase(it);
                        con->expected_seq++;
                    }
                }
                else if (seq > con->expected_seq)
                {
                    con->out_of_order[seq] = vector<char>(payload, payload + paylen);
                }

                send_ack(con, con->expected_seq);
            }
        }

        pthread_mutex_unlock(&con->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    if (listen_sock == -1)
    {
        listen_sock = socket(AF_INET, SOCK_DGRAM, 0);

        int yes = 1;
        setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ip;
        addr.sin_port = port;
        int rc = bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr));
        assert(rc == 0);
    }

    struct connection *con = new struct connection();
    con->conn_id = conn_counter++;
    con->expected_seq = 0;
    con->max_recv_buf = recv_window_bytes;
    pthread_mutex_init(&con->con_lock, NULL);

    char buf[MAX_SEGMENT_SIZE];
    struct sockaddr_in peer;
    socklen_t peerlen = sizeof(peer);

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    while (1)
    {
        int n = recvfrom(listen_sock, buf, MAX_SEGMENT_SIZE, 0,
                         (struct sockaddr *)&peer, &peerlen);
        n = poli_verify(buf, n);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr))
            continue;

        poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;
        if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != POLI_TYPE_SYN)
            continue;

        bool already_have_it = false;
        for (map<int, struct connection *>::iterator it = cons.begin();
             it != cons.end(); ++it)
        {
            struct connection *other = it->second;
            if (other->servaddr.sin_addr.s_addr == peer.sin_addr.s_addr &&
                other->servaddr.sin_port == peer.sin_port)
            {

                struct sockaddr_in bound;
                socklen_t bl = sizeof(bound);
                getsockname(other->sockfd, (struct sockaddr *)&bound, &bl);

                send_synack(other->sockfd, &other->servaddr, other->conn_id,
                            other->max_recv_buf, bound.sin_port);
                already_have_it = true;
                break;
            }
        }
        if (already_have_it)
            continue;

        break;
    }

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in mine;
    memset(&mine, 0, sizeof(mine));
    mine.sin_family = AF_INET;
    mine.sin_addr.s_addr = INADDR_ANY;
    mine.sin_port = 0;
    int rc = bind(con->sockfd, (struct sockaddr *)&mine, sizeof(mine));
    assert(rc == 0);

    socklen_t ml = sizeof(mine);
    getsockname(con->sockfd, (struct sockaddr *)&mine, &ml);
    uint16_t my_port = mine.sin_port;

    con->servaddr = peer;

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    while (1)
    {
        send_synack(con->sockfd, &con->servaddr, con->conn_id,
                    con->max_recv_buf, my_port);

        int n = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, NULL, NULL);
        n = poli_verify(buf, n);
        if (n < (int)sizeof(poli_tcp_ctrl_hdr))
            continue;

        poli_tcp_ctrl_hdr *r = (poli_tcp_ctrl_hdr *)buf;
        if (r->protocol_id != POLI_PROTOCOL_ID)
            continue;
        if (r->type == POLI_TYPE_ACK || r->type == POLI_TYPE_DATA)
            break;
    }

    struct timeval off = {0, 0};
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &off, sizeof(off));

    /* Publish the new connection atomically with respect to the handler thread:
       it must never observe an incremented fdmax before cons/data_fds/timer_fds
       are fully populated. */
    pthread_mutex_lock(&registry_lock);
    cons.insert({con->conn_id, con});

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 1;
    spec.it_value.tv_nsec = 0;
    spec.it_interval.tv_sec = 1;
    spec.it_interval.tv_nsec = 0;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;
    pthread_mutex_unlock(&registry_lock);

    DEBUG_PRINT("Connection established!");

    return con->conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    recv_window_bytes = recv_buffer_bytes;

    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
