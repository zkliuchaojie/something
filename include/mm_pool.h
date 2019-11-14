#ifndef MM_POOL_H_
#define MM_POOL_H_

#ifndef MM_EPOCH_H_
#include "mm_epoch.h"
#endif

#ifndef MM_PARTITION_H_
#include "mm_partition.h"
#endif

#ifndef MM_GARBAGE_LIST_H_
#include "mm_garbage_list.h"
#endif

#ifndef STH_H_
#include "sth.h"
#endif

// template <typename T>
// class AbstractPool {
// public:
//     AbstractPool() {} = 0;
//     virtual ~AbstractPool() {};
//     virtual T *Alloc() = 0;
//     virtual void Free(T * object) = 0;
//     virtual void Protect() = 0;
//     virtual void Unprotect() = 0;
// };

/*
 * PO means ptm object.
 */
template <typename T>
class Pool {
public:
    Pool() {
        partitions_ = (Partition *)malloc(sizeof(Partition) * kPartitionNum);
        for(int i=0; i<kPartitionNum; i++) {
            new (&partitions_[i]) Partition(&epoch_manager_);
        }

        array_ = (T *)malloc(sizeof(T) * kPoolSize);
        int num_per_partition = kPoolSize/kPartitionNum;
        Partition *partition;
        int index;
        for(int i=0; i<kPartitionNum; i++) {
            partition = &partitions_[i];
            for(int j=0; j<num_per_partition; j++) {
                index = i*num_per_partition + j;
                new (&array_[index]) T();
                array_[index].__owner_partition_ = partition;
                array_[index].po_pool_ = this;
                array_[index].__next_= (T *)partition->free_list_;
                partition->free_list_ = &array_[index];
            }
        }
        tail_.store(0, std::memory_order_seq_cst);
    }
    
    ~Pool() {
        if(array_ != nullptr) {
            free(array_);
        }
        if(partitions_ != nullptr) {
            free(partitions_);
        }
    }

    T* Alloc() {
        thread_local Partition *thread_partition = nullptr;
        if(thread_partition == nullptr) {
            word slot = tail_.fetch_add(1, std::memory_order_seq_cst)%kPartitionNum;
            thread_partition = &partitions_[slot];
        }

        //try to reclaim T from garbage_list 
        while(thread_partition->free_list_ == nullptr) {
            thread_partition->garbage_list_->epoch_manager_->BumpCurrentEpoch();
            thread_partition->garbage_list_->TrytoReclaim();
            if(thread_partition->free_list_ == nullptr)
                return nullptr;
        }
        T *retval = (T *)thread_partition->free_list_;
        thread_partition->free_list_ = retval->__next_;
        thread_partition->allocated_num_++;
        if(thread_partition->allocated_num_ > \
            (thread_partition->num_per_partition_*1/2)) {
            //std::cout << thread_des_partition->allocated_des_ << std::endl;
            thread_partition->garbage_list_->TrytoReclaim();
        }
        return retval;
    }
    void Free(T * object) {
        object->__owner_partition_->garbage_list_->Push(object, T::Free);
    }
    void Protect() {
        epoch_manager_.Protect();
    }
    void Unprotect() {
        epoch_manager_.Unprotect();
    }
private:
    EpochManager epoch_manager_;
    T *array_;
    Partition *partitions_;
    std::atomic<word> tail_;
};

#endif