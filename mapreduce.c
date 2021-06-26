#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <pthread.h>
#include "mergesort.c"

node_head **partitions;
pthread_key_t glob_var_key;

void push_list(
    char *key,
    char *value, node_head *head)
{
  node_t *node = (node_t *)malloc(sizeof(node_t));

  node->key = key;
  node->value = value;
  node->processed = 0;

  pthread_mutex_lock(&head->lock);
  if (head->next == NULL)
  {
    node->next = NULL;
    head->next = node;
  }
  else
  {
    node->next = head->next;
    head->next = node;
  }
  pthread_mutex_unlock(&head->lock);
}

char *get_next(char *key, int partition_number)
{
  node_head *head = partitions[partition_number];
  char *return_key = NULL;
  pthread_mutex_lock(&head->lock);

  if (head)
  {
    node_t *node = head->next;
    while (node)
    {
      if (strcmp(node->key, key) == 0 && node->processed == 0)
      {
        node->processed = 1;
        return_key = node->key;
      }
      node = node->next;
    }
  }
  pthread_mutex_unlock(&head->lock);
  return return_key;
}

void init_partitions(int num)
{
  for (int i = 0; i < num; i++)
  {
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    node_head *head = (node_head *)malloc(sizeof(node_head));
    head->next = NULL;
    head->lock = lock;
    partitions[i] = head;
  }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

void MR_Emit(char *key, char *value)
{
  unsigned long hashIndex = MR_DefaultHashPartition(key, num_partitions);
  push_list(key, value, partitions[hashIndex]);
}

void *map_helper(mapper_args_t *args)
{
  pthread_mutex_t *lock = &args->lock;
  while (1)
  {
    char *curr_filename;
    pthread_mutex_lock(&lock);

    if (args->num_files <= args->curr_file)
    {
      pthread_mutex_unlock(&lock);
      return NULL;
    }
    curr_filename = args->files[args->curr_file];
    args->curr_file = args->curr_file + 1;
    pthread_mutex_unlock(&lock);
    mapper(curr_filename);
  }
}

void *reduce_helper(reducer_args_t *args)
{
  pthread_mutex_t *lock = &args->lock;
  while (1)
  {
    pthread_mutex_lock(&lock);
    if (args->num_partitions <= args->curr_partition)
    {
      pthread_mutex_unlock(&lock);
      return NULL;
    }

    int *p = malloc(sizeof(int));
    *p = args->curr_partition;
    pthread_setspecific(glob_var_key, p);
    args->curr_partition++;
    pthread_mutex_unlock(&lock);

    int *glob_spec_var = pthread_getspecific(glob_var_key);
    MergeSort(&partitions[*glob_spec_var]->next);

    node_t *iterator = partitions[*glob_spec_var]->next;
    while (iterator != NULL)
    {
      reducer(iterator->key, get_next, *glob_spec_var);
      iterator = partitions[*glob_spec_var]->next;
    }
    free(p);
  }
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
  num_partitions = num_reducers;

  pthread_t mapper_threads[num_mappers];
  pthread_t reducer_threads[num_reducers];

  mapper_args_t mapper_args = {
    curr_file : 0,
    num_files : argc - 1,
    files : &argv[1],
    lock : PTHREAD_MUTEX_INITIALIZER
  };

  reducer_args_t reducer_args = {
    curr_partition : 0,
    num_partitions : num_partitions,
    partitions : partitions,
    lock : PTHREAD_MUTEX_INITIALIZER
  };

  init_partitions(num_partitions);

  for (int i = 0; i < num_mappers; i++)
  {
    if (i < mapper_args.num_files)
      Pthread_create(&mapper_threads[i], NULL, map_helper, &mapper_args);
  }

  for (int i = 0; i < num_mappers; i++)
  {
    if (i < mapper_args.num_files)
      pthread_join(mapper_threads[i], NULL);
  }

  pthread_key_create(&glob_var_key, NULL);
  for (int i = 0; i < num_reducers; i++)
  {
    if (i < reducer_args.num_partitions)
      Pthread_create(&reducer_threads[i], NULL, reduce_helper, &reducer_args);
  }
  for (int i = 0; i < num_reducers; i++)
  {
    if (i < reducer_args.num_partitions)
      pthread_join(reducer_threads[i], NULL);
  }

  for (int i = 0; i < num_partitions; i++)
  {
    node_t *iterator = partitions[i]->next;
    node_t *tmp;

    free(partitions[i]);
    while (iterator != NULL)
    {
      free(iterator);
      tmp = iterator;
      iterator = iterator->next;
      free(tmp);
    }
  }
};