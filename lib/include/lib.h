#pragma once

#include <cstdint>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <deque>
#include <map>
#include <vector>
#include "utils.h"
#include "protocol.h"

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

/* One sent segment kept in the sender's window until it gets ACKed. */
struct sent_segment
{
    char data[MAX_SEGMENT_SIZE];
    int len;
    uint16_t seq;
    struct timespec send_time; /* last time we sent (or resent) this segment */
};

/* Protocol control block. Used for both sender and receiver sides.
 * Some fields are only useful on one side. */
struct connection
{
    int sockfd;
    int conn_id;
    struct sockaddr_in servaddr; /* peer address */
    pthread_mutex_t con_lock;
    int max_window_seq;

    /* Sender state */
    uint16_t next_seq;                /* next seq number to assign */
    uint16_t base_seq;                /* first unacked seq */
    std::deque<sent_segment> unacked; /* sent but not ACKed yet */
    int peer_window;                  /* receiver advertised window */

    /* Receiver state */
    uint16_t expected_seq;                              /* next in-order seq */
    std::map<uint16_t, std::vector<char>> out_of_order; /* buffered out-of-order */
    std::deque<char> recv_buf;                          /* ready to deliver to app */
    int max_recv_buf;
};

/* ########## API that we expose to the application ########### */

int wait4connect(uint32_t ip, uint16_t port);
int setup_connection(uint32_t ip, uint16_t port);
int recv_data(int connectionid, char *buffer, int len);
int send_data(int conn_id, char *buffer, int len);
void init_receiver(int recv_buffer_bytes);
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
