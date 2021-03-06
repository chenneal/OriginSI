#ifndef MASTER_H_
#define MASTER_H_

#define THREADNUM threadnum
#define NODENUM nodenum

#define MSEND_BUFFER_MAXSIZE 1000
#define MRECV_BUFFER_MAXSIZE 8

#define LINEMAX 20

#define LISTEN_QUEUE 500

typedef enum server_command
{
   cmd_starttransaction,
   cmd_getendtimestamp,
   cmd_updateprocarray,
   cmd_release_master
} master_command;

typedef struct master_arg
{
   int index;
   int conn;
} master_arg;

extern int oneNodeWeight;
extern int twoNodeWeight;
extern int redo_limit;

extern void InitMasterBuffer(void);
extern void InitMessage(void);
extern void InitParam(void);
extern void InitMaster(void);
extern void InitRecord(void);
extern void InitNetworkParam(void);

extern int nodenum;
extern int threadnum;
extern int record_port;
extern int client_port;
extern int master_port;
extern int message_port;
extern int param_port;
extern char master_ip[20];

extern uint64_t ** msend_buffer;
extern uint64_t ** mrecv_buffer;
extern pthread_t * master_tid;

#endif
