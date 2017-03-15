#include <stdlib.h>
#include <sys/socket.h>
#include <stdio.h>
#include "type.h"
#include "master.h"
#include "master_data.h"
#include "procarray.h"

void ProcessStartTransaction(uint64_t *recv_buffer, int conn, int mindex)
{
   int index;
   int count;
   int max;
   int min;
   pthread_t pid;
   int size;

   size = 3 + 1 + 1 + MAXPROCS;

   TransactionId tid;
   TimeStampTz starttime;

   uint32_t* buf;
   index = recv_buffer[1];
   pid = recv_buffer[2];

   tid = AssignTransactionId();
   starttime = SetCurrentTransactionStartTimestamp();

   ProcArrayAdd(index, tid, pid);

   buf = (uint32_t*)msend_buffer[mindex];
   GetServerTransactionSnapshot(index, &count, &min, &max, buf+5);

   *(buf) = count;
   *(buf+1) = min;
   *(buf+2) = max;
   *(buf+3) = starttime;
   *(buf+4) = tid;

   if (send(conn, msend_buffer[mindex], size*sizeof(uint32_t), 0) == -1)
	   printf("process start transaction send error\n");
}

void ProcessEndTimestamp(uint64_t *recv_buffer, int conn, int mindex)
{
   TimeStampTz endtime;

   endtime = SetCurrentTransactionStopTimestamp();

   *(msend_buffer[mindex]) = endtime;

   if (send(conn, msend_buffer[mindex], sizeof(uint64_t), 0) == -1)
	   printf("process end time stamp send error\n");
}

void ProcessUpdateProcarray(uint64_t *recv_buffer, int conn, int mindex)
{
	int index;
	int status = 1;

	index = recv_buffer[1];

	AtEnd_ProcArray(index);

	*(msend_buffer[mindex]) = status;

	if (send(conn, msend_buffer[mindex], sizeof(uint64_t), 0) == -1)
	   printf("process end time stamp send error\n");
}
