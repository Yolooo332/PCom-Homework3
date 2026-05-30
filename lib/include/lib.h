#pragma once

#include <cstdint>
#include <cstring>
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
/* Every segment ends with a 4 byte CRC32 so we can catch packets that got
   their bits flipped on the wire and just drop them (the multi-connection
   tests turn on netem corruption). The headers from protocol.h stay exactly
   as they are, the checksum just tags along at the very end. */
#define POLI_CSUM_SIZE 4
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr) + POLI_CSUM_SIZE)

#define MAX_CONNECTIONS 32

static inline uint32_t poli_crc32(const void *buf, int len)
{
    const unsigned char *p = (const unsigned char *)buf;
    uint32_t crc = 0xFFFFFFFFu;
    for (int i = 0; i < len; i++) {
        crc ^= p[i];
        for (int k = 0; k < 8; k++)
            crc = (crc & 1u) ? (crc >> 1) ^ 0xEDB88320u : (crc >> 1);
    }
    return crc ^ 0xFFFFFFFFu;
}

/* stick the checksum on the end of buf and return the new total length */
static inline int poli_seal(char *buf, int len)
{
    uint32_t crc = poli_crc32(buf, len);
    memcpy(buf + len, &crc, POLI_CSUM_SIZE);
    return len + POLI_CSUM_SIZE;
}

/* check the trailing checksum. gives back the length without it, or -1 if the
   packet is too short or the crc doesn't match (so it's corrupted, throw it) */
static inline int poli_verify(const char *buf, int n)
{
    if (n < POLI_CSUM_SIZE)
        return -1;
    uint32_t crc;
    memcpy(&crc, buf + n - POLI_CSUM_SIZE, POLI_CSUM_SIZE);
    if (crc != poli_crc32(buf, n - POLI_CSUM_SIZE))
        return -1;
    return n - POLI_CSUM_SIZE;
}

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

/* Guards the connection registry (cons / data_fds / timer_fds / fdmax), which is
   shared between the application thread (which registers connections) and the I/O
   handler thread (which polls/iterates it). Defined once in libcommon.cpp. */
extern pthread_mutex_t registry_lock;
