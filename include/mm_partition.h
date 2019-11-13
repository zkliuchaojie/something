#ifndef MM_PARTITION_H_
#define MM_PARTITION_H_

#ifndef MM_GARBAGE_LIST_H_
#include "mm_garbage_list.h"
#endif

// the current implementation do not consider reclaim descriptor partitions.
class Partition {
public:
    void *free_list_; // pointing to a MMAbstractObject
    unsigned long int allocated_num_;
    GarbageList *garbage_list_;
    unsigned long int num_per_partition_;
    Partition(EpochManager *epoch_manager) {
        garbage_list_ = new GarbageList(epoch_manager);
        free_list_ = nullptr;
        allocated_num_ = 0;
        num_per_partition_ = kPoolSize/kPartitionNum;
    }
private:
};

#endif