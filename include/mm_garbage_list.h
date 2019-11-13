#ifndef MM_GARBAGE_LIST_H_
#define MM_GARBAGE_LIST_H_

#ifndef MM_EPOCH_H_
#include "mm_epoch.h"
#endif

class GarbageEntry {
public:
    GarbageEntry() {
        removed_epoch_ = 0;
        removed_object_ = nullptr;
    }
    epoch removed_epoch_;
    DestroyCallback destroy_callback_;
    void *removed_object_;
};

class GarbageList {
public: 
    EpochManager* epoch_manager_;
    GarbageList() = delete;
    GarbageList(EpochManager *epoch_manager);
    ~GarbageList();
    void Push(void* removed_object, DestroyCallback destroy_callback);
    unsigned int TrytoReclaim();
    word GetTail();
private:
    GarbageEntry *garbage_entries_;
    unsigned long int tail_;
};

GarbageList::GarbageList(EpochManager* epoch_manager) {
    epoch_manager_ = epoch_manager;
    garbage_entries_ = (GarbageEntry*)malloc(sizeof(GarbageEntry)*kGarbageListCount);
    for(int i=0; i<kGarbageListCount; i++) {
        new (&garbage_entries_[i]) GarbageEntry();
    }
    tail_ = 0;
}

GarbageList::~GarbageList() {
    for(int i=0; i<kGarbageListCount; i++) {
        GarbageEntry *garbage_entry = &garbage_entries_[i];
        if(garbage_entry->removed_object_ != nullptr) {
            garbage_entry->destroy_callback_(garbage_entry->removed_object_);
            garbage_entry->removed_object_ = nullptr;
            garbage_entry->removed_epoch_ = 0;
        }
    }
    if(garbage_entries_ != nullptr) {
        free(garbage_entries_);
    }
    garbage_entries_ = nullptr;
}

void GarbageList::Push(void* removed_object, DestroyCallback destroy_callback) {
    epoch current_epoch = epoch_manager_->GetCurrentEpoch();
    GarbageEntry *garbage_entry;
    for(;;) {
        unsigned long int slot = (tail_++)%kGarbageListCount;
        if( slot%(kPoolSize/kPartitionNum/4) == 0) {
            epoch_manager_->BumpCurrentEpoch();
        }

        garbage_entry = &garbage_entries_[slot];
        if(garbage_entry->removed_epoch_ != 0) {
            if(!epoch_manager_->IsSafeToReclaim(garbage_entry->removed_epoch_)) {
                continue;
            }
            garbage_entry->destroy_callback_(garbage_entry->removed_object_);
        }
        garbage_entry->removed_epoch_ = current_epoch;
        garbage_entry->destroy_callback_ = destroy_callback;
        garbage_entry->removed_object_ = removed_object;
        return;
    }
}

// the return value is the number of objects reclaimed
unsigned int GarbageList::TrytoReclaim() {
    unsigned int reclaimed_objects = 0;
    GarbageEntry *garbage_entry;
    for(int i=0; i<kGarbageListCount; i++) {
        garbage_entry = &garbage_entries_[i];
        // removed_epoch_ equals zero which means 
        // there is no object in this slot
        if(garbage_entry->removed_epoch_ == 0) {
            continue;
        }
        // we can not reclaim this object
        if(!epoch_manager_->IsSafeToReclaim(garbage_entry->removed_epoch_)) {
            continue;
        }
        garbage_entry->destroy_callback_(garbage_entry->removed_object_);
        // reset the entry
        garbage_entry->removed_epoch_ = 0;
        garbage_entry->removed_object_ = nullptr;
        reclaimed_objects++;
    }
    return reclaimed_objects;
}

word GarbageList::GetTail() {
    return tail_;
}

#endif