#ifndef SOCKET_H_
#define SOCKET_H_

#include <pthread.h>
#include "type.h"
#define LINEMAX 20
#define RECEIVE_BUFFSIZE 8
#define LISTEN_QUEUE 800
#define NODENUM nodenum
#define THREADNUM threadnum
#define NODENUMMAX 50
#define THREADNUMMAX 64
#define IP_PREFIX "192.168.0."

#define SEND_BUFFER_MAXSIZE 1000
#define RECV_BUFFER_MAXSIZE 1000

#define SSEND_BUFFER_MAXSIZE 8
#define SRECV_BUFFER_MAXSIZE 1000

extern void InitParamClient(void);
extern void InitMessageClient(void);
extern void InitServer(int nid);
extern void InitClient(int nid, int threadid);
extern void InitMasterClient(int threadid);
extern void InitRecordClient(void);

extern int record_port;
extern int ip_suffix;

extern int message_socket;
extern int param_socket;
extern int port_base;

extern int nodenum;
extern int threadnum;
// the ID of the node
extern int nodeid;
extern int master_port;
extern int message_port;
extern int param_port;
int recordfd;

// store the connect socket to the other nodes in the distributed system.
extern int connect_socket[NODENUMMAX][THREADNUMMAX];
extern int server_socket[THREADNUMMAX];
extern pthread_t * server_tid;

// record the related ip
extern char master_ip[20];
extern char local_ip[20];
extern char node_ip[NODENUMMAX][20];

typedef struct server_arg
{
   int index;
   int conn;
} server_arg;

extern uint64_t ** send_buffer;
extern uint64_t ** recv_buffer;
extern uint64_t ** ssend_buffer;
extern uint64_t ** srecv_buffer;

extern void InitNetworkParam(void);
extern void WaitDataReady(void);
extern void GetParam(void);
extern void InitClientBuffer(void);
extern void InitServerBuffer(void);

#endif
