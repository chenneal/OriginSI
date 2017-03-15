/*
 * trans.h
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

#ifndef TRANS_H_
#define TRANS_H_

#include "type.h"
#include "proc.h"
#include "timestamp.h"

#define InvalidTransactionId ((TransactionId)0)

struct TransactionData
{
	TransactionId tid;

	TimeStampTz starttime;

	TimeStampTz stoptime;

	bool trans_snap[NODENUMMAX];

	bool pure_insert[NODENUMMAX];
};

typedef struct TransactionData TransactionData;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern void InitTransactionStructMemAlloc(void);

extern void setPureInsert(int nid);

extern void TransactionLoadData(int i);

extern void TransactionRunSchedule(void* args);

extern void TransactionContextCommit(TransactionId tid, TimeStampTz ctime, int index);

extern void TransactionContextAbort(TransactionId tid, int index);

extern void StartTransaction(void);

extern void CommitTransaction(void);

extern void AbortTransaction(void);

extern int PreCommit(void);

extern int LocalPreCommit(int* number, int index, TransactionId tid);

extern void LocalCommitTransaction(int index, TimeStampTz ctime);

extern void LocalAbortTransaction(int index, int trulynum);

extern int GetNodeId(int index);

extern int GetLocalIndex(int index);
#endif /* TRANS_H_ */
