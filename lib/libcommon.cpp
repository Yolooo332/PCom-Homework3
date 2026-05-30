#include <sys/socket.h>
#include <sys/types.h>
#include <sys/timerfd.h>
#include <netinet/ip.h>
#include <stdio.h>
#include <cstdint>
#include "lib.h"
#include <vector>
#include <map>
#include <cstring>
#include <assert.h>
#include <unistd.h>
#include <cstdlib>
#include <pthread.h>
#include <sys/poll.h>
#include <fcntl.h>

using namespace std;

/* We use extern to use the variables defined either in
   librecv or libsend */
extern std::map<int, struct connection *> &cons;
extern struct pollfd data_fds[MAX_CONNECTIONS];
extern struct pollfd timer_fds[MAX_CONNECTIONS];
extern int fdmax;

/* Single lock protecting the connection registry. The application thread mutates
   cons / data_fds / timer_fds / fdmax while registering connections; this thread
   polls and iterates them. Without this lock those are concurrent reads/writes
   (and a std::map mutated mid-iteration) -> undefined behaviour, which is what
   broke the multiple-connection transfers. */
pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;

/* Look up the connection id owning a socket fd. Caller must NOT hold the lock. */
static int conn_id_for_fd(int fd)
{
    int j = -1;
    pthread_mutex_lock(&registry_lock);
    for (auto const &x : cons)
    {
        if (x.second->sockfd == fd)
        {
            j = x.first;
            break;
        }
    }
    pthread_mutex_unlock(&registry_lock);
    return j;
}

int recv_message_or_timeout(char *buff, size_t len, int *conn_id)
{
    int i;

    /* Take a consistent snapshot of the poll set under the registry lock so we
       never read a half-registered connection (a partially written pollfd, or an
       fdmax that is ahead of data_fds[].events). recvfrom() itself runs outside
       the lock so a slow socket can't block a connection from being registered. */
    struct pollfd dfds[MAX_CONNECTIONS];
    struct pollfd tfds[MAX_CONNECTIONS];
    int local_fdmax;

    pthread_mutex_lock(&registry_lock);
    local_fdmax = fdmax;
    for (i = 0; i < local_fdmax; i++)
    {
        dfds[i] = data_fds[i];
        tfds[i] = timer_fds[i];
    }
    pthread_mutex_unlock(&registry_lock);

    if (local_fdmax == 0)
        return -14;

    /* We check if data is available on a socket or if a timer has expired */
    if (poll(dfds, local_fdmax, 0) < 0)
        return -14;
    if (poll(tfds, local_fdmax, 0) < 0)
        return -14;

    for (i = 0; i < local_fdmax; i++)
    {
        /* Data available on a socket */
        if (dfds[i].revents & POLLIN)
        {

            struct sockaddr_in servaddr;
            socklen_t slen = sizeof(struct sockaddr_in);

            int n = recvfrom(dfds[i].fd, buff, len, MSG_WAITALL, (struct sockaddr *)&servaddr, &slen);

            /* Find which connection this socket coresponds to */
            int j = conn_id_for_fd(dfds[i].fd);
            if (j == -1)
                return -14; /* not mapped (yet); just try again next round */

            /* Write in the conn_id the connection of the socket*/
            *conn_id = j;
            return n;
        }
        /* A timer has expired on a connection */
        if (tfds[i].revents & POLLIN)
        {

            char dummybuf[8];
            ssize_t r = read(tfds[i].fd, dummybuf, 8);
            (void)r;

            /* Same connection as the matching data socket at this index */
            int j = conn_id_for_fd(dfds[i].fd);
            if (j == -1)
                return -14;

            /* Write in the conn_id the connection of the socket*/
            *conn_id = j;
            return -1;
        }
    }

    /* If nothing happened, return -14. */
    return -14;
}
