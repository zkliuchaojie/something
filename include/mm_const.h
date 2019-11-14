#ifndef MM_CONST_H_
#define MM_CONST_H_

typedef unsigned long int word;
typedef unsigned long int tid;
typedef unsigned long int epoch;
typedef void (*DestroyCallback)(void *object);
const int kGarbageListCount = 128 * 1024;
const int kPoolSize = 64 * 1024;
// each thread occupies one partition
const int kPartitionNum = 1;
// each thread occupies one epoch entry
const int kEpochTableSize = 128;

#endif