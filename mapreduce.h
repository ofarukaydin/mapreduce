#ifndef __mapreduce_h__
#define __mapreduce_h__

typedef struct node
{
  char *key;
  char *value;
  struct node *next;
  int processed;
} node_t;

typedef struct _node_head
{
  pthread_mutex_t lock;
  node_t *next;
} node_head;

node_t *head = NULL;
int num_partitions;

typedef struct _mapper_data
{
  int curr_file;
  int num_files;
  char *files;
  pthread_mutex_t lock;
} mapper_args_t;

typedef struct _reducer_data
{
  int curr_partition;
  int num_partitions;
  pthread_mutex_t lock;
  node_head **partitions;
} reducer_args_t;

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition);

#endif // __mapreduce_h__