#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "lock.h"
#include "master.h"
#include "procarray.h"

void InitSys(void)
{
   InitMasterBuffer();
   InitLock();
   InitTransactionIdAssign();
   InitProc();
}
int main()
{
   pid_t pid1, pid2, pid3;
   InitNetworkParam();

   if((pid1 = fork()) < 0)
   {
      printf("fork error\n");
   }
   if (pid1 == 0)
   {
	   InitParam();
	   printf("parameter server end\n");
	   exit(1);
   }

   if((pid2 = fork()) < 0)
   {
      printf("fork error\n");
   }

   if (pid2 == 0)
   {
	   InitMessage();
	   printf("message server end\n");
	   exit(1);
   }

   if((pid3 = fork()) < 0)
   {
      printf("fork error\n");
   }

   if (pid3 == 0)
   {
	   InitRecord();
	   printf("record server end\n");
	   exit(1);
   }

   InitSys();
   InitMaster();
   printf("master server end\n");
   return 0;
}
