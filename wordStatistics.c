#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>

/*Structure for updater and mapper buffer*/
struct mapper_pool
{
    char word[50][100];
};

/*Structure for mapper and reducer buffer*/
struct reducer_pool
{
    char word[50][100];
};

/*Structure to hold o/p values for reducer*/
struct count_value
{
   int count;
   char value[200];
}cv;


/*Global Variables*/
FILE *fp;
pthread_mutex_t mutex;
pthread_mutex_t mutex1;
pthread_mutex_t mutex2;
pthread_cond_t empty;
pthread_cond_t empty1;
pthread_cond_t empty2;
pthread_cond_t full;
pthread_cond_t full1;
pthread_cond_t full2;
struct mapper_pool totalwords;
struct mapper_pool totalwords_red;
int num=0;
int num1=0;
int in=0;
int in1=0;
int out =0;
int out1=0;
int N =20;//buffer size 0f 20
int N1 =20;//buffer size 0f 20
int leftover =0;
int leftover1=0;
int update_complete =0;
int map_complete =0;
int reducer_complete=0;

char buf[200];
char words[200];
size_t nbytes = sizeof(buf);;
int fd;
char str[200];
char temp[200];
int i=0,j=0,flag=0,found=0;
char *token= &temp[0];
//token = &temp[0];
struct count_value cv1[200];


/**********************************************************************************/

/*Updater Function*/
void *updater(void *threadid)
{ 
	//printf("Inside updater\n");   
	long tid;
   tid = (long)threadid;
   char word1[50],*pch;

while(1)
{

   if(fgets(word1,50,fp)!=NULL)
   {		//printf("Inside while of updater\n");
			pthread_mutex_lock(&mutex);		   
			
		while (num == N) 
  		{	
			//printf("Waiting inside while of updater with num==N\n");	
			pthread_cond_wait(&full, &mutex);
   	} 
		
		pch=strtok(word1,"\n");
		/*char *charvalue = totalwords[i].word;
		char *pchvalue = pch;
    	int ascii_of_pre = (int)*charvalue;
		int ascii_of_current = (int)*pchvalue;
		if (ascii_of_current > ascii_of_pre)
			{	
				//i++;
				printf("\n");
				//printf("\nStructure number %d\n",i);
			}*/

		strcpy(totalwords.word[in],pch);
  		in = (in + 1) % N ;
  		num++;
 
		if (num == 1)
			pthread_cond_broadcast(&empty);

		pthread_mutex_unlock (&mutex);
	}
	else
	{
		if(num>0)
		{
			pthread_cond_broadcast(&empty);
		}
		else 
		{
			break;
		}
	}
}
	printf("Terminating Updater thread\n");
	update_complete=1;
	leftover = num;
	pthread_cond_broadcast(&empty);
   pthread_exit(NULL);
}

/**********************************************************************************/

/*Mapper Function*/
void *mapper(void *threadid_m)
{
	//printf("Inside mapper\n");	
	long tid;
   tid = (long)threadid_m;
	char map[50];	
	char temp1 [200];

while(1)
{
	if (update_complete!=1 || leftover!=0)
	{
	//printf("Inside while of mapper\n");
	pthread_mutex_lock(&mutex); 	
	while (num == 0 && update_complete ==0) 
	{  
		//printf("Waiting inside while of mapper with num==0\n");	
		pthread_cond_wait(&empty, &mutex);
  	}

   strcpy(map,totalwords.word[out]);
	out = (out + 1) % N;
   num--;
	strcpy(temp1,"(");
	strcat(temp1,map);
	strcat(temp1,",1)");
	//printf("%s\n",temp1);
	pthread_mutex_lock(&mutex1);	
	while (num1 == N1) 
  	{	
		//printf("Waiting inside while of mapper inside while with num1==N1\n");				
		pthread_cond_wait(&full1, &mutex1);
   }  
	strcpy(totalwords_red.word[in1],temp1);
   in1 = (in1 + 1) % N1; 
   num1++;
 
  if (num1 == 1)
     pthread_cond_broadcast(&empty1);


	if (num == N -1)
      pthread_cond_broadcast(&full);

	pthread_mutex_unlock(&mutex1);

	//printf("Value of num: %d",num);
	pthread_mutex_unlock(&mutex);
	}

	else
	{
		if(num1>0)
		{
			pthread_cond_broadcast(&empty1);
		}
		else 
		{
			break;
		}
	}
	
	}
   printf("Terminating Mapper thread[%ld]\n",tid);
	map_complete=1;
	leftover1 = num1;
	pthread_cond_broadcast(&empty1);
   pthread_exit(NULL);
}

/**********************************************************************************/

/*Reducer Function*/
void *reducer(void *threadid_r)
{
	//printf("Inside reducer\n");	
	long tid;
   tid = (long)threadid_r;

	while (map_complete!=1 || leftover1!=0)
	{
		//printf("Inside while of reducer\n");		
		pthread_mutex_lock(&mutex1);  	
		while (num1 == 0 && map_complete==0) 
		{  //printf("Waiting inside while of reducer inside while with num1==0\n");	
			pthread_cond_wait(&empty1, &mutex1);
  		}
		//pthread_mutex_lock(&mutex2);
		strcpy(buf,totalwords_red.word[out1]);
   	out1 = (out1 + 1) % N1;
   	num1--;

		token = strtok(buf,"(,1)\n");//tokenizing the input 
		found=0;

		if (flag==0)//will execute only once when first word is encountered
      	{
			strcpy(cv1[i].value,token);//assigning word as first element in list
			cv1[i].count=1;//with count 1
			//printf("(%s,%i)\n",cv1[i].value,cv1[i].count);//printing the output on screen
			i++;
			flag=1;
			found=1;
      	}
      	else
      	{
			j = 0;
			while(j<i)
			{
				if(strcmp(token,cv1[j].value) == 0)//comparing the word with existing elements
				{
					cv1[j].count += 1;
					found=1;
					//printf("(%s,%d)\n",cv1[j].value,cv1[j].count);//printing the output on screen	
					break;
				}
				j++;
			}
		}
      
		if(found==0)//adding the word to list if not previously present in the list
      	{
		     strcpy(cv1[i].value,token);
		     cv1[i].count=1;//with count 1
				//printf("(%s,%i)\n",cv1[i].value,cv1[i].count);//printing the output on screen	
		     i++;
      	}
		
		if (num1 == N1 -1)
     	pthread_cond_broadcast(&full1);
		pthread_cond_broadcast(&empty2);
		//pthread_mutex_unlock(&mutex2);
   	pthread_mutex_unlock(&mutex1);
		//printf("%d\n",i);
    	}
	
	reducer_complete =1;	
	printf("Terminating Reducer thread[%ld]\n",tid);
	pthread_cond_broadcast(&empty2);
   pthread_exit(NULL);
}

/**********************************************************************************/

/*wordCount Function*/
void *wordCount (void *threadcount)
{
	long tid;
	tid = (long)threadcount;
	int listcount=i;
	FILE *fp_wordcount;
	fp_wordcount = fopen("wordCount.txt","w");
	
	while (reducer_complete != 1) 
	{  
		//printf("Waiting inside while of wordcount with reducer complete==0\n");	
		pthread_cond_wait(&empty2, &mutex2);
  	}

	//if (reducer_complete==1)
	//{
		//printf("Inside IF of wordCount\n");		
		//pthread_mutex_lock(&mutex2);
	   //printf("Inside IF of wordCount\n");	
		//printf("Value of i: %d\n",i);
		listcount=i;
		//printf("Value of listcount: %d\n",listcount);
		for (i=0;i<listcount;i++)
			{
				fprintf(fp_wordcount,"(%s,%i)\n",cv1[i].value,cv1[i].count);
			}
		//pthread_mutex_unlock(&mutex2);
	//}

	printf("Terminating WordCount thread\n");
   pthread_exit(NULL);
}

/**********************************************************************************/
/*Main Function*/
int main(int argc, char *argv[])
{
   int upd, maps,redc,wordc,j,k,l;
   long t,x,y;
   int mapper_threads;
	int reducer_threads;

	mapper_threads = atoi(argv[2]);
	reducer_threads = atoi(argv[3]);
	//summarizer_thread = atoi(argv[4]);

	pthread_t threads;
   pthread_t threadsMap[mapper_threads];
   pthread_t threadsRed[reducer_threads];
	pthread_t thread_wordcount;

   pthread_mutex_init(&mutex, NULL);
   pthread_mutex_init(&mutex1, NULL);
   pthread_mutex_init(&mutex2, NULL);
   pthread_cond_init (&empty, NULL);
   pthread_cond_init (&empty1, NULL);
   pthread_cond_init (&empty2, NULL);
   pthread_cond_init (&full, NULL);
   pthread_cond_init (&full1, NULL);
   pthread_cond_init (&full2, NULL);    

   fp= fopen(argv[1],"r");

   //for(t=0; t<NUM_THREADS; t++){
      printf("In main: creating updater thread\n");
      upd = pthread_create(&threads, NULL, updater, NULL);
      if (upd){
         printf("ERROR; return code from pthread_create() is %d\n", upd);
         exit(-1);
      }
   //}

	for(x=0; x<mapper_threads; x++){
      printf("In main: creating mapper thread : %ld\n", x);
      maps = pthread_create(&threadsMap[x], NULL, mapper, (void*)x);
      if (maps){
         printf("ERROR; return code from pthread_create() is %d\n", maps);
         exit(-1);
      }
   }

	for(y=0; y<reducer_threads; y++){
      printf("In main: creating reducer thread: %ld\n", y);
      redc = pthread_create(&threadsRed[y], NULL, reducer, (void*)y);
      if (redc){
         printf("ERROR; return code from pthread_create() is %d\n", redc);
         exit(-1);
      }
   }

	printf("In main: creating wordCount thread\n");
      wordc = pthread_create(&thread_wordcount, NULL, wordCount, NULL);
      if (wordc){
         printf("ERROR; return code from pthread_create() is %d\n", wordc);
         exit(-1);
      }
		
	pthread_join(threads, NULL);

	for (k =0; k<mapper_threads; k++){
		pthread_join(threadsMap[k], NULL);}

	for (l =0; l<reducer_threads; l++){
		pthread_join(threadsRed[l], NULL);}

	pthread_join(thread_wordcount, NULL);

   pthread_mutex_destroy(&mutex);
   pthread_mutex_destroy(&mutex1);
	pthread_mutex_destroy(&mutex2);
   //pthread_exit(NULL);
	/*int listcount=i;//printing output on screen
	printf("\n");
	FILE *fp_wordcount;
	fp_wordcount = fopen("wordCount.txt","w");
   for (i=0;i<listcount;i++)
   {
   	fprintf(fp_wordcount,"(%s,%i)\n",cv1[i].value,cv1[i].count);//printing the output on screen
  	}*/
   fclose(fp);
	printf("--Process completed. Please check wordcount.txt--\n");
   return(0);
}

