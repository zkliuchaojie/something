/*
 * File:
 *   intset.c
 * Author(s):
 *   Pascal Felber <pascal.felber@unine.ch>
 *   Patrick Marlier <patrick.marlier@unine.ch>
 * Description:
 *   Integer set stress test.
 *
 * Copyright (c) 2007-2014.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * This program has a dual license and can also be distributed
 * under the terms of the MIT license.
 */

#ifndef INTSET_H_
#include "intset.h"
#endif

#ifndef INTSET_HASH_TABLE_H_
#include "hashtable.h"
#endif

// #ifndef INTSET_LINKEDLIST_H_
// #include "linkedlist.h"
// #endif

// #ifndef INTSET_BST_H_
// #include "bst.h"
// #endif

/*
#ifndef INTSET_RBT_H_
#include "rbt.h"
#endif
*/

#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>

#define RO                              1
#define RW                              0

#if defined(TM_GCC) 
# include "../../abi/gcc/tm_macros.h"
#elif defined(TM_DTMC) 
# include "../../abi/dtmc/tm_macros.h"
/* Make erand48 pure for DTMC (transaction_pure should work too). */
static double tanger_wrapperpure_erand48(unsigned short int __xsubi[3]) __attribute__ ((weakref("erand48")));
#elif defined(TM_INTEL)
# include "../../abi/intel/tm_macros.h"
#elif defined(TM_ABI)
# include "../../abi/tm_macros.h"
#endif /* defined(TM_ABI) */

#if defined(TM_GCC) || defined(TM_DTMC) || defined(TM_INTEL) || defined(TM_ABI)
# define TM_COMPILER
/* Add some attributes to library function */
TM_PURE 
void exit(int status);
TM_PURE 
void perror(const char *s);
#else /* Compile with explicit calls to tinySTM */

#endif /* Compile with explicit calls to tinySTM */

#ifdef DEBUG
# define IO_FLUSH                       fflush(NULL)
/* Note: stdio is thread-safe */
#endif

#define DEFAULT_DURATION                10000
#define DEFAULT_INITIAL                 256
#define DEFAULT_NB_THREADS              1
#define DEFAULT_RANGE                   (DEFAULT_INITIAL * 2)
#define DEFAULT_SEED                    0
#define DEFAULT_UPDATE                  20

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

/* ################################################################### *
 * GLOBALS
 * ################################################################### */
static volatile int stop;
static unsigned short main_seed[3];

static inline void rand_init(unsigned short *seed)
{
  seed[0] = (unsigned short)rand();
  seed[1] = (unsigned short)rand();
  seed[2] = (unsigned short)rand();
}

static inline int rand_range(int n, unsigned short *seed)
{
  /* Return a random number in range [0;n) */
  int v = (int)(erand48(seed) * n);
  assert (v >= 0 && v < n);
  return v;
}

typedef struct thread_data {
  AbstractIntset *set;
  struct barrier *barrier;
  unsigned long nb_add;
  unsigned long nb_remove;
  unsigned long nb_contains;
  unsigned long nb_found;
  unsigned long nb_abort;
  unsigned long nb_read_abort;
#ifdef ORIGINAL_INTERFACE_TL2_H_
  double nb_global_clock_overhead;
#endif
  unsigned short seed[3];
  int diff;
  int range;
  int update;
  int alternate;
#ifdef USE_LINKEDLIST
  int unit_tx;
#endif /* LINKEDLIST */
  char padding[64];
} thread_data_t;

int set_add(AbstractIntset *set, Value_t val, void *not_used_for_now) {
    return set->Insert(val, val);
}

int set_remove(AbstractIntset *set, Value_t val, void *not_used_for_now) {
    return set->Delete(val);
}

int set_contains(AbstractIntset *set, Value_t val, void *not_used_for_now) {
    return (int)set->Get(val);
}

AbstractIntset *set_new() {
#ifdef INTSET_HASH_TABLE_H_
    return new HashTable();
#else
#ifdef INTSET_LINKEDLIST_H_
    return new LinkedList();
#else
#ifdef INTSET_BST_H_
    return new Bst();
#else
#ifdef INTSET_RBT_H_
    return new Rbt();
#endif
#endif
#endif
#endif
}

unsigned long long set_size(AbstractIntset *intset) {
    return intset->Size();
}

void set_delete(AbstractIntset *intset) {
    delete intset;
}

/* ################################################################### *
 * BARRIER
 * ################################################################### */

typedef struct barrier {
  pthread_cond_t complete;
  pthread_mutex_t mutex;
  int count;
  int crossing;
} barrier_t;

static void barrier_init(barrier_t *b, int n)
{
  pthread_cond_init(&b->complete, NULL);
  pthread_mutex_init(&b->mutex, NULL);
  b->count = n;
  b->crossing = 0;
}

static void barrier_cross(barrier_t *b)
{
  pthread_mutex_lock(&b->mutex);
  /* One more thread through */
  b->crossing++;
  /* If not all here, wait */
  if (b->crossing < b->count) {
    pthread_cond_wait(&b->complete, &b->mutex);
  } else {
    pthread_cond_broadcast(&b->complete);
    /* Reset for next time */
    b->crossing = 0;
  }
  pthread_mutex_unlock(&b->mutex);
}

/* ################################################################### *
 * STRESS TEST
 * ################################################################### */

static void *test(void *data)
{
    Value_t val;
    int op, last = -1;
    thread_data_t *d = (thread_data_t *)data;

  /* Wait on barrier */
  barrier_cross(d->barrier);

  while (stop == 0) {
    op = rand_range(100, d->seed);
    if (op < d->update) {
      if (d->alternate) {
        /* Alternate insertions and removals */
        if (last < 0) {
          /* Add random value */
          val = rand_range(d->range, d->seed) + 1;
          if (set_add(d->set, val, d)) {
            d->diff++;
            last = val;
          }
          d->nb_add++;
        } else {
          /* Remove last value */
          if (set_remove(d->set, last, d))
            d->diff--;
          d->nb_remove++;
          last = -1;
        }
      } else {
        /* Randomly perform insertions and removals */
        val = rand_range(d->range, d->seed) + 1;
        if ((op & 0x01) == 0) {
          /* Add random value */
          if (set_add(d->set, val, d))
            d->diff++;
          d->nb_add++;
        } else {
          /* Remove random value */
          if (set_remove(d->set, val, d))
            d->diff--;
          d->nb_remove++;
        }
      }
    } else {
      /* Look for random value */
      val = rand_range(d->range, d->seed) + 1;
      if (set_contains(d->set, val, d))
        d->nb_found++;
      d->nb_contains++;
    }
  }
  d->nb_abort = thread_abort_counter;
  d->nb_read_abort = thread_read_abort_counter;
#ifdef ORIGINAL_INTERFACE_TL2_H_
  d->nb_global_clock_overhead = thread_global_clock_overhead;
#endif
  PTM_THREAD_CLEAN;
  return NULL;
}

int main(int argc, char **argv)
{
  struct option long_options[] = {
    // These options don't set a flag
    {"help",                      no_argument,       NULL, 'h'},
    {"do-not-alternate",          no_argument,       NULL, 'a'},
#ifndef TM_COMPILER
    {"contention-manager",        required_argument, NULL, 'c'},
#endif /* ! TM_COMPILER */
    {"duration",                  required_argument, NULL, 'd'},
    {"initial-size",              required_argument, NULL, 'i'},
    {"num-threads",               required_argument, NULL, 'n'},
    {"range",                     required_argument, NULL, 'r'},
    {"seed",                      required_argument, NULL, 's'},
    {"update-rate",               required_argument, NULL, 'u'},
#ifdef USE_LINKEDLIST
    {"unit-tx",                   no_argument,       NULL, 'x'},
#endif /* LINKEDLIST */
    {NULL, 0, NULL, 0}
  };

  AbstractIntset *set;
  int i, c, val, size, ret;
  unsigned long reads, updates;
#ifndef TM_COMPILER
  char *s;
  unsigned long aborts, aborts_1, aborts_2,
    aborts_locked_read, aborts_locked_write,
    aborts_validate_read, aborts_validate_write, aborts_validate_commit,
    aborts_invalid_memory, aborts_killed,
    locked_reads_ok, locked_reads_failed, max_retries;
  //stm_ab_stats_t ab_stats;
#endif /* ! TM_COMPILER */
  thread_data_t *data;
  pthread_t *threads;
  pthread_attr_t attr;
  barrier_t barrier;
  struct timeval start, end;
  struct timespec timeout;
  unsigned long duration = DEFAULT_DURATION;
  unsigned long initial = DEFAULT_INITIAL;
  unsigned long nb_threads = DEFAULT_NB_THREADS;
  unsigned long range = DEFAULT_RANGE;
  unsigned long seed = DEFAULT_SEED;
  unsigned long update = DEFAULT_UPDATE;
  unsigned alternate = 1;
#ifndef TM_COMPILER
  char *cm = NULL;
#endif /* ! TM_COMPILER */
#ifdef USE_LINKEDLIST
  int unit_tx = 0;
#endif /* LINKEDLIST */
  sigset_t block_set;

  while(1) {
    i = 0;
    c = getopt_long(argc, argv, "ha"
#ifndef TM_COMPILER
                    "c:"
#endif /* ! TM_COMPILER */
                    "d:i:n:r:s:u:"
#ifdef USE_LINKEDLIST
                    "x"
#endif /* LINKEDLIST */
                    , long_options, &i);

    if(c == -1)
      break;

    if(c == 0 && long_options[i].flag == 0)
      c = long_options[i].val;

    switch(c) {
     case 0:
       /* Flag is automatically set */
       break;
     case 'h':
       printf("intset -- STM stress test "
#if defined(USE_LINKEDLIST)
              "(linked list)\n"
#elif defined(USE_RBTREE)
              "(red-black tree)\n"
#elif defined(USE_SKIPLIST)
              "(skip list)\n"
#elif defined(USE_HASHSET)
              "(hash set)\n"
#endif /* defined(USE_HASHSET) */
              "\n"
              "Usage:\n"
              "  intset [options...]\n"
              "\n"
              "Options:\n"
              "  -h, --help\n"
              "        Print this message\n"
              "  -a, --do-not-alternate\n"
              "        Do not alternate insertions and removals\n"
#ifndef TM_COMPILER
	      "  -c, --contention-manager <string>\n"
              "        Contention manager for resolving conflicts (default=suicide)\n"
#endif /* ! TM_COMPILER */
	      "  -d, --duration <int>\n"
              "        Test duration in milliseconds (0=infinite, default=" XSTR(DEFAULT_DURATION) ")\n"
              "  -i, --initial-size <int>\n"
              "        Number of elements to insert before test (default=" XSTR(DEFAULT_INITIAL) ")\n"
              "  -n, --num-threads <int>\n"
              "        Number of threads (default=" XSTR(DEFAULT_NB_THREADS) ")\n"
              "  -r, --range <int>\n"
              "        Range of integer values inserted in set (default=" XSTR(DEFAULT_RANGE) ")\n"
              "  -s, --seed <int>\n"
              "        RNG seed (0=time-based, default=" XSTR(DEFAULT_SEED) ")\n"
              "  -u, --update-rate <int>\n"
              "        Percentage of update transactions (default=" XSTR(DEFAULT_UPDATE) ")\n"
#ifdef USE_LINKEDLIST
              "  -x, --unit-tx\n"
              "        Use unit transactions\n"
#endif /* LINKEDLIST */
         );
       exit(0);
     case 'a':
       alternate = 0;
       break;
#ifndef TM_COMPILER
     case 'c':
       cm = optarg;
       break;
#endif /* ! TM_COMPILER */
     case 'd':
       duration = atoi(optarg);
       break;
     case 'i':
       initial = atoi(optarg);
       break;
     case 'n':
       nb_threads = atoi(optarg);
       break;
     case 'r':
       range = atoi(optarg);
       break;
     case 's':
       seed = atoi(optarg);
       break;
     case 'u':
       update = atoi(optarg);
       break;
#ifdef USE_LINKEDLIST
     case 'x':
       unit_tx++;
       break;
#endif /* LINKEDLIST */
     case '?':
       printf("Use -h or --help for help\n");
       exit(0);
     default:
       exit(1);
    }
  }

  assert(duration >= 0);
  assert(initial >= 0);
  assert(nb_threads > 0);
  assert(range > 0 && range >= initial);
  assert(update >= 0 && update <= 100);

#if defined(USE_LINKEDLIST)
  printf("Set type     : linked list\n");
#elif defined(USE_RBTREE)
  printf("Set type     : red-black tree\n");
#elif defined(USE_SKIPLIST)
  printf("Set type     : skip list\n");
#elif defined(USE_HASHSET)
  printf("Set type     : hash set\n");
#endif /* defined(USE_HASHSET) */
#ifndef TM_COMPILER
  printf("CM           : %s\n", (cm == NULL ? "DEFAULT" : cm));
#endif /* ! TM_COMPILER */
  printf("Duration     : %ld\n", duration);
  printf("Initial size : %ld\n", initial);
  printf("Nb threads   : %ld\n", nb_threads);
  printf("Value range  : %ld\n", range);
  printf("Seed         : %ld\n", seed);
  printf("Update rate  : %ld\n", update);
  printf("Alternate    : %d\n", alternate);
#ifdef USE_LINKEDLIST
  printf("Unit tx      : %d\n", unit_tx);
#endif /* LINKEDLIST */
  printf("Type sizes   : int=%d/long=%d/ptr=%d/word=%d\n",
         (int)sizeof(int),
         (int)sizeof(long),
         (int)sizeof(void *),
         (int)sizeof(size_t));

  timeout.tv_sec = duration / 1000;
  timeout.tv_nsec = (duration % 1000) * 1000000;

  if ((data = (thread_data_t *)malloc(nb_threads * sizeof(thread_data_t))) == NULL) {
    perror("malloc");
    exit(1);
  }
  if ((threads = (pthread_t *)malloc(nb_threads * sizeof(pthread_t))) == NULL) {
    perror("malloc");
    exit(1);
  }

  if (seed == 0)
    srand((int)time(NULL));
  else
    srand(seed);

  set = set_new();

  stop = 0;

  /* Thread-local seed for main thread */
  rand_init(main_seed);
  if (alternate == 0 && range != initial * 2)
    printf("WARNING: range is not twice the initial set size\n");

  /* Populate set */
  printf("Adding %ld entries to set\n", initial);
  i = 0;
  while (i < initial) {
    val = rand_range(range, main_seed) + 1;
    if (set_add(set, val, 0))
      i++;
  }
    size = set_size(set);
    printf("Set size     : %d\n", size);
  PTM_THREAD_CLEAN;
  /* Access set from all threads */
  barrier_init(&barrier, nb_threads + 1);
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (i = 0; i < nb_threads; i++) {
    printf("Creating thread %d\n", i);
    data[i].range = range;
    data[i].update = update;
    data[i].alternate = alternate;
#ifdef USE_LINKEDLIST
    data[i].unit_tx = unit_tx;
#endif /* LINKEDLIST */
    data[i].nb_add = 0;
    data[i].nb_remove = 0;
    data[i].nb_contains = 0;
    data[i].nb_found = 0;
    data[i].nb_abort = 0;
    data[i].nb_read_abort = 0;
  #ifdef ORIGINAL_INTERFACE_TL2_H_
    data[i].nb_global_clock_overhead = 0;
  #endif
    data[i].diff = 0;
    rand_init(data[i].seed);
    data[i].set = set;
    data[i].barrier = &barrier;
    if (pthread_create(&threads[i], &attr, test, (void *)(&data[i])) != 0) {
      fprintf(stderr, "Error creating thread\n");
      exit(1);
    }
  }
  pthread_attr_destroy(&attr);

  /* Start threads */
  barrier_cross(&barrier);

  printf("STARTING...\n");
  gettimeofday(&start, NULL);
  if (duration > 0) {
    nanosleep(&timeout, NULL);
  } else {
    sigemptyset(&block_set);
    sigsuspend(&block_set);
  }
  stop = 1;
  gettimeofday(&end, NULL);
  printf("STOPPING...\n");

  /* Wait for thread completion */
  for (i = 0; i < nb_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Error waiting for thread completion\n");
      exit(1);
    }
  }

  duration = (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000);

  reads = 0;
  updates = 0;
  for (i = 0; i < nb_threads; i++) {
    printf("Thread %d\n", i);
    printf("  #add        : %lu\n", data[i].nb_add);
    printf("  #remove     : %lu\n", data[i].nb_remove);
    printf("  #contains   : %lu\n", data[i].nb_contains);
    printf("  #found      : %lu\n", data[i].nb_found);
    printf("  #abort      : %lu\n", data[i].nb_abort);
    printf("  #read abort : %lu\n", data[i].nb_read_abort);
#ifdef ORIGINAL_INTERFACE_TL2_H_
    printf("  #global clock overhead: %f\n", data[i].nb_global_clock_overhead);
#endif
    reads += data[i].nb_contains;
    updates += (data[i].nb_add + data[i].nb_remove);
    size += data[i].diff;
  }
  printf("Set size      : %lld (expected: %d)\n", set_size(set), size);
  ret = (set_size(set) != size);
  printf("Duration      : %ld (ms)\n", duration);
  printf("#txs          : %lu (%f / s)\n", reads + updates, (reads + updates) * 1000.0 / duration);
  printf("#read txs     : %lu (%f / s)\n", reads, reads * 1000.0 / duration);
  printf("#update txs   : %lu (%f / s)\n", updates, updates * 1000.0 / duration);

  /* Delete set */
  set_delete(set);

  free(threads);
  free(data);

  return ret;
}
