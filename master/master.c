#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "procarray.h"
#include "master.h"
#include "type.h"
#include "master_data.h"

int nodenum;
int threadnum;
int record_port;
int client_port;
int master_port;
int message_port;
int param_port;
char master_ip[20];

// send buffer for server respond.
uint64_t ** msend_buffer;
uint64_t ** mrecv_buffer;

pthread_t * master_tid;

int oneNodeWeight;
int twoNodeWeight;
int redo_limit;

void* MasterRespond(void *sockp);

int ReadConfig(char * find_string, char * result)
{
   int i;
   int j;
   int k;
   FILE * fp;
   char buffer[30];
   char * p;
   if ((fp = fopen("config.txt", "r")) == NULL)
   {
       printf("can not open the configure file.\n");
       fclose(fp);
       return -1;
   }
   for (i = 0; i < LINEMAX; i++)
   {
      if (fgets(buffer, sizeof(buffer), fp) == NULL)
         continue;
      for (p = find_string, j = 0; *p != '\0'; p++, j++)
      {
         if (*p != buffer[j])
            break;
      }

      if (*p != '\0' || buffer[j] != ':')
      {
         continue;
      }

      else
      {
         k = 0;
         // jump over the character ':' and the space character.
         j = j + 2;
         while (buffer[j] != '\0')
         {
            *(result+k) = buffer[j];
            k++;
            j++;
         }
         *(result+k) = '\0';
         fclose(fp);
         return 1;
      }
   }
   fclose(fp);
   printf("can not find the configure you need\n");
   return -1;
}

void InitMasterBuffer(void)
{
   int i;
   msend_buffer = (uint64_t **) malloc (NODENUM*(THREADNUM+1) * sizeof(uint64_t *));
   mrecv_buffer = (uint64_t **) malloc (NODENUM*(THREADNUM+1) * sizeof(uint64_t *));

   if (msend_buffer == NULL || mrecv_buffer == NULL)
	   printf("master buffer pointer malloc error\n");

   for (i = 0; i < NODENUM*(THREADNUM+1); i++)
   {
      msend_buffer[i] = (uint64_t *) malloc (MSEND_BUFFER_MAXSIZE * sizeof(uint64_t));
      mrecv_buffer[i] = (uint64_t *) malloc (MRECV_BUFFER_MAXSIZE * sizeof(uint64_t));
	   if ((msend_buffer[i] == NULL) || mrecv_buffer[i] == NULL)
		   printf("master buffer malloc error\n");
   }
}

void InitParam(void)
{
	int ip[NODENUM];
	//record the accept socket fd of the connected client
	int conn;
	int param_connect[NODENUM];

	int param_send_buffer[10+NODENUM];
	int param_recv_buffer[1];

	int master_sockfd;
    int port = param_port;
	// use the TCP protocol
	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("parameter server bind error!\n");
		exit(1);
	}
	//listen

    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("parameter server listen error!\n");
        exit(1);
    }

	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;
   	int j;
    while(i < NODENUM)
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	param_connect[i] = conn;

    	if(conn < 0)
    	{
    		printf("param master accept connect error!\n");
    	    exit(1);
    	}
    	i++;
    }

    for (j = 0; j < NODENUM; j++)
    {
    	int ret;
    	ret = recv(param_connect[j], param_recv_buffer, sizeof(param_recv_buffer), 0);
    	if (ret == -1)
    	    printf("param master recv error\n");
    	ip[j] = param_recv_buffer[0];
    }

    for (j = 0; j < NODENUM; j++)
    {
    	param_send_buffer[0] = NODENUM;
    	param_send_buffer[1] = THREADNUM;
    	param_send_buffer[2] = client_port;
    	param_send_buffer[3] = message_port;
    	param_send_buffer[4] = master_port;
    	param_send_buffer[5] = record_port;
    	param_send_buffer[6] = j;
    	param_send_buffer[7] = oneNodeWeight;
    	param_send_buffer[8] = twoNodeWeight;
    	param_send_buffer[9] = redo_limit;
    	int k;
    	for (k = 0; k < NODENUM; k++)
    	{
    		param_send_buffer[10+k] = ip[k];
    	}
    	int ret;
    	int size = (10+NODENUM)*sizeof(int);
    	ret = send(param_connect[j], param_send_buffer, size, 0);
    	if (ret == -1)
    	    printf("param naster send error\n");
        close(param_connect[j]);
    }
}

void InitMessage(void)
{
	// record the accept socket fd of the connected client
	int conn;

	int message_connect[NODENUM];
	int message_buffer[1];

	int master_sockfd;
    int port = message_port;

	// use the TCP protocol
	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("message server bind error!\n");
		exit(1);
	}

	//listen
    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("message server listen error!\n");
        exit(1);
    }

	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;

    while(i < NODENUM)
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	message_connect[i] = conn;
    	if(conn < 0)
    	{
    		printf("message master accept connect error!\n");
    	    exit(1);
    	}
    	i++;
    }

    int j;
    int ret;

    // inform the node in the system begin start the transaction process.
    for (j = 0; j < NODENUM; j++)
    {
    	ret = recv(message_connect[j], message_buffer, sizeof(message_buffer), 0);
    	if (ret == -1)
    		printf("message master recv error\n");
    }

    for (j = 0; j < NODENUM; j++)
    {
    	message_buffer[0] = 999;
    	ret = send(message_connect[j], message_buffer, sizeof(message_buffer), 0);
    	if (ret == -1)
    		printf("message master send error\n");
    	close(message_connect[j]);
    }
    //now we can reform the node to begin to run the transaction.
}

void InitRecord(void)
{
	//record the accept socket fd of the connected client
	int conn;

	int record_connect[NODENUM];

	int master_sockfd;
    int port = record_port;

	// use the TCP protocol
	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("record server bind error!\n");
		exit(1);
	}

	//listen
    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("record server listen error!\n");
        exit(1);
    }

	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;

    while(i < NODENUM)
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	record_connect[i] = conn;
    	if(conn < 0)
    	{
    		printf("record master accept connect error!\n");
    	    exit(1);
    	}
    	i++;
    }

    int j;
    int ret;

    uint64_t **buf;
    buf = (uint64_t**)malloc(NODENUM*sizeof(uint64_t*));
    for (j = 0; j < NODENUM; j++)
    {
    	buf[j] = (uint64_t*)malloc(5*sizeof(uint64_t));
    }

    // inform the node in the system begin start the transaction process.
    for (j = 0; j < NODENUM; j++)
    {
    	ret = recv(record_connect[j], buf[j], 5*sizeof(uint64_t), 0);
    	if (ret == -1)
    		printf("record master recv error\n");
    }

    for (i = 0; i < NODENUM; i++)
    	for (j = 0; j < NODENUM-i-1; j++)
    	{
    		uint64_t temp[5];
    		if (buf[j][0] > buf[j+1][0])
    		{
    			temp[0] = buf[j][0];
    			temp[1] = buf[j][1];
    			temp[2] = buf[j][2];
    			temp[3] = buf[j][3];
    			temp[4] = buf[j][4];
    			buf[j][0] = buf[j+1][0];
    			buf[j][1] = buf[j+1][1];
    			buf[j][2] = buf[j+1][2];
    			buf[j][3] = buf[j+1][3];
    			buf[j][4] = buf[j+1][4];
                buf[j+1][0] = temp[0];
                buf[j+1][1] = temp[1];
                buf[j+1][2] = temp[2];
                buf[j+1][3] = temp[3];
                buf[j+1][4] = temp[4];
    		}
    	}

    // flush the buffer
    printf("\n");
    for (i = 0; i < NODENUM; i++)
    {
    	printf("ip%ld %ld %ld %ld %ld\n", buf[i][0], buf[i][1], buf[i][2], buf[i][3], buf[i][4]);
    }
    //now we can reform the node to begin to run the transaction.
}

void InitMaster(void)
{
	int status;
	//record the accept socket fd of the connected client
	int conn;

	master_arg *argu = (master_arg *) malloc (NODENUM*(THREADNUM+1)*sizeof(master_arg));
	master_tid = (pthread_t *) malloc (NODENUM*(THREADNUM+1)*sizeof(pthread_t));

	int master_sockfd;
	void * pstatus;
    int port = master_port;
	// use the TCP protocol

	master_sockfd = socket(AF_INET, SOCK_STREAM, 0);
	//bind
	struct sockaddr_in mastersock_addr;
	memset(&mastersock_addr, 0, sizeof(mastersock_addr));
	mastersock_addr.sin_family = AF_INET;
	mastersock_addr.sin_port = htons(port);
	mastersock_addr.sin_addr.s_addr = INADDR_ANY;

	if (bind(master_sockfd, (struct sockaddr *)&mastersock_addr, sizeof(mastersock_addr)) == -1)
	{
		printf("master server bind error!\n");
		exit(1);
	}

	//listen
    if(listen(master_sockfd, LISTEN_QUEUE) == -1)
    {
        printf("master server listen error!\n");
        exit(1);
    }
    //receive or transfer data
	socklen_t slave_length;
	struct sockaddr_in slave_addr;
	slave_length = sizeof(slave_addr);
   	int i = 0;

    while(i < NODENUM*(THREADNUM+1))
    {
    	conn = accept(master_sockfd, (struct sockaddr*)&slave_addr, &slave_length);
    	argu[i].conn = conn;
    	argu[i].index = i;
    	if(conn < 0)
    	{
    		printf("master accept connect error!\n");
    	    exit(1);
    	}
    	status = pthread_create(&master_tid[i], NULL, MasterRespond, &(argu[i]));
    	if (status != 0)
    		printf("create thread %d error %d!\n", i, status);
    	i++;
    }

    for (i = 0; i < NODENUM*(THREADNUM+1); i++)
       pthread_join(master_tid[i],&pstatus);
}

void* MasterRespond(void *pargu)
{
	int conn;
	int index;
	master_arg * temp;
	temp = (master_arg *) pargu;
	conn = temp->conn;
	index = temp->index;

	memset(mrecv_buffer[index], 0, MRECV_BUFFER_MAXSIZE*sizeof(uint64_t));
	int ret;
	master_command type;
    do
    {
    	ret = recv(conn, mrecv_buffer[index], MRECV_BUFFER_MAXSIZE*sizeof(uint64_t), 0);
   	    if (ret == -1)
   	    {
   	    	printf("master receive error!\n");
   	    }

   	    type = (master_command)(*(mrecv_buffer[index]));
   	    switch(type)
   	    {
   	       case cmd_starttransaction:
   	    	   ProcessStartTransaction(mrecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_getendtimestamp:
   	    	   ProcessEndTimestamp(mrecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_updateprocarray:
   	    	   ProcessUpdateProcarray(mrecv_buffer[index], conn, index);
   	    	   break;
   	       case cmd_release_master:
   	    	   printf("enter release master conncet\n");
   	    	   break;
   	       default:
   	    	   printf("error route, never here!\n");
   	    	   break;
   	    }
    } while (type != cmd_release_master);
	close(conn);
    pthread_exit(NULL);
    return (void*)NULL;
}

void InitNetworkParam(void)
{
   char buffer[5];

   ReadConfig("threadnum", buffer);
   threadnum = atoi(buffer);

   ReadConfig("nodenum", buffer);
   nodenum = atoi(buffer);

   ReadConfig("clientport", buffer);
   client_port = atoi(buffer);

   ReadConfig("masterport", buffer);
   master_port = atoi(buffer);

   ReadConfig("paramport", buffer);
   param_port = atoi(buffer);

   ReadConfig("messageport", buffer);
   message_port = atoi(buffer);

   ReadConfig("recordport", buffer);
   record_port = atoi(buffer);

   ReadConfig("masterip", master_ip);

   ReadConfig("oneweight", buffer);
   oneNodeWeight = atoi(buffer);

   ReadConfig("twoweight", buffer);
   twoNodeWeight = atoi(buffer);

   ReadConfig("redolimit", buffer);
   redo_limit = atoi(buffer);
}
