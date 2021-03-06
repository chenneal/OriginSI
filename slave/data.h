/*
 * data.h
 *
 *  Created on: Jan 29, 2016
 *      Author: Yu
 */

#ifndef DATA_H_
#define DATA_H_
#include<stdbool.h>
#include"type.h"
#include"timestamp.h"
#include "snapshot.h"

#define TABLENUM  9
#define VERSIONMAX 20

#define InvalidTupleId (TupleId)(0)

/* add by yu the data structure for the tuple */

/* Version is used for store a version of a record */
typedef struct {
	TransactionId tid;
	TimeStampTz committime;
	bool deleted;
	/* to stock other information of each version. */
	TupleId value;
} Version;

/*  Record is a multi-version tuple structure */
typedef struct {
	TupleId tupleid;
	int rear;
	int front;
	int lcommit;
	Version VersionList[VERSIONMAX];
} Record;

/* THash is pointer to a hash table for every table */
typedef Record * THash;

typedef int VersionId;

/* the lock in the tuple is used to verify the atomic operation of transaction */
extern pthread_rwlock_t* RecordLock[TABLENUM];

/* just use to verify the atomic operation of a short-time */
extern pthread_spinlock_t* RecordLatch[TABLENUM];

/* every table will have a separated HashTable */
extern Record* TableList[TABLENUM];

extern int BucketNum[TABLENUM];
extern int BucketSize[TABLENUM];

extern int RecordNum[TABLENUM];;

extern bool MVCCVisible(Record * r, VersionId v, Snapshot * snap);
extern void ProcessInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessTrulyInsert(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessUpdate(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessRead(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessSnapshot(uint64_t * recv_buffer, int conn, int index);

extern int TrulyDataInsert(int table_id, int h, TupleId tuple_id, TupleId value, int index, TransactionId tid);
extern int TrulyDataUpdate(int table_id, int h, TupleId tuple_id, TupleId value, int index, TransactionId tid);
extern int TrulyDataDelete(int table_id, int h, TupleId tuple_id, int index, TransactionId tid);

extern void ProcessPrepare(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessCommit(uint64_t * recv_buffer, int conn, int sindex);
extern void ProcessAbort(uint64_t * recv_buffer, int conn, int sindex);

#endif
