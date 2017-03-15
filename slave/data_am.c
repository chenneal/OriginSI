/*
 * data_am.c
 *
 *  Created on: Dec 7, 2015
 *      Author: xiaoxin
 */

/*
 * interface for data access method.
 */

#include<pthread.h>
#include<assert.h>
#include<stdbool.h>
#include<sys/socket.h>
#include<assert.h>
#include"config.h"
#include"data_am.h"
#include"data_record.h"
#include"lock_record.h"
#include"thread_global.h"
#include"proc.h"
#include"trans.h"
#include"snapshot.h"
#include"transactions.h"
#include"data.h"
#include"socket.h"
#include"communicate.h"

/*
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int index;
	int status;
	DataRecord datard;
	THREAD* threadinfo;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	/*
	 * the node transaction process must to get the data from the storage process in the
	 * node itself or in the other nodes, both use the socket to communicate.
	 */

	int lindex;
	lindex = GetLocalIndex(index);
    if ((Send4(lindex, nid, cmd_insert, table_id, tuple_id, value)) == -1)
       printf("insert send error!\n");
    if ((Recv(lindex, nid, 1)) == -1)
       printf("insert recv error!\n");

    status = *(recv_buffer[lindex]);

    if (status == 0)
    	return 0;

	datard.node_id = nid;
	DataRecordInsert(&datard);

	return 1;
}

/*
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid)
{
	int index=0;
	DataRecord datard;

    int status;
	THREAD* threadinfo;

	bool isdelete = false;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	if (Send5(lindex, nid, cmd_update, table_id, tuple_id, value, isdelete) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update find recv error\n");

	status = *(recv_buffer[lindex]);

    if (status == 0)
    	return 0;

    setPureInsert(nid);
	/* record the updated data. */
    datard.node_id = nid;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @return:'0' for not found, '1' for success, '-1' for update-conflict-rollback.
 */

int Data_Delete(int table_id, TupleId tuple_id, int nid)
{
	int index=0;
	DataRecord datard;
    uint64_t value = InvalidTupleId;
    int status;
	THREAD* threadinfo;

	bool isdelete = true;

	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	if (Send5(lindex, nid, cmd_update, table_id, tuple_id, value, isdelete) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 1) == -1)
		printf("update find recv error\n");

	status = *(recv_buffer[lindex]);

    if (status == 0)
    	return 0;

    setPureInsert(nid);
	/* record the updated data. */
    datard.node_id = nid;
	DataRecordInsert(&datard);
	return 1;
}

/*
 * @input:'isupdate':true for reading before updating, false for commonly reading.
 * @return:NULL for read nothing, to rollback or just let it go.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag)
{
	int index;
	uint64_t value;
	TransactionData* tdata;
	THREAD* threadinfo;
	*flag=1;
	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

	if (tdata->trans_snap[nid] == false)
	{
		TransSnapshot(nid);
	}

	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);

	if (Send3(lindex, nid, cmd_read, table_id, tuple_id) == -1)
		printf("read find send error\n");
	if (Recv(lindex, nid, 2) == -1)
		printf("read find recv error\n");

    *flag = *(recv_buffer[lindex]);
    value = *(recv_buffer[lindex]+1);


    if (*flag != 1)
    {
        return 0;
    }

    else
    {
    	return value;
    }

	return 0;
}

void TransSnapshot(int nid)
{
	int size;
	int i;
	int index;
	THREAD* threadinfo;
	Snapshot* snap;
	TransactionData* tdata;
	size = MAXPROCS + 4;
	/* get the pointer to current thread information. */
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	/* get the pointer to transaction-snapshot-data. */
	snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

	assert(tdata->trans_snap[nid] == false);

	int lindex;
	lindex = GetLocalIndex(index);

	uint32_t* buf = (uint32_t*)send_buffer[lindex];
	*(buf) = cmd_snapshot;
	*(buf+1) = snap->tcount;
	*(buf+2) = snap->tid_min;
	*(buf+3) = snap->tid_max;
    for (i = 0; i < MAXPROCS; i++)
    	*(buf+4+i) = snap->tid_array[i];

	if (send(connect_socket[nid][lindex], send_buffer[lindex], size*sizeof(uint32_t), 0) == -1)
	{
		printf("send snapshot error\n");
	}

	if (Recv(lindex, nid, 1) == -1)
	{
		printf("recv snap error\n");
	}

	// transfer the snapshot just one time.
    tdata->trans_snap[nid] = true;
}
