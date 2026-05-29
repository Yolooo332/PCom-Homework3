#pragma once

#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

/* Segment types */
#define POLI_TYPE_DATA 1
#define POLI_TYPE_ACK 2
#define POLI_TYPE_SYN 3
#define POLI_TYPE_SYNACK 4

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr
{
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr
{
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};
