#ifndef MM_EPOCH_H_
#define MM_EPOCH_H_

#ifndef MM_CONST_H_
#include "mm_const.h"
#endif

#include <atomic>
#include <iostream>
#include <thread>

class EpochManager;

// the current implementation do not consider reclaim entries.
class EpochEntry {
public:
    EpochEntry();
    EpochEntry(std::atomic<epoch> protected_epoch);
    std::atomic<epoch> protected_epoch_;
};

// this is for recording each thread epoch
class EpochTable {
public:
    EpochTable();
    ~EpochTable();
    void Protect(epoch current_epoch);
    void Unprotect();
    bool IsProtected();
    epoch ComputeNewSafeToReclaimEpoch(epoch current_epoch);
private:
    EpochEntry *epoch_entries_;
    std::atomic<word> tail_;
    EpochEntry *GetThreadEpochEntry();
};

class EpochManager {
public:
    EpochManager();
    void Protect();
    void Unprotect();
    bool IsProtected();
    bool IsSafeToReclaim(epoch removed_epoch);
    epoch GetCurrentEpoch();
    void SetCurrentEpoch(epoch current_epoch);
    void BumpCurrentEpoch();
    epoch GetSafeToReclaimEpoch();

private:
    std::atomic<epoch> current_epoch_;
    EpochTable epoch_table_;
    std::atomic<epoch> safe_to_reclaim_epoch_;
    void ComputeNewSafeToReclaimEpoch(epoch current_epoch);
};


EpochManager::EpochManager() {
    current_epoch_.store(1, std::memory_order_relaxed);
    safe_to_reclaim_epoch_.store(0, std::memory_order_relaxed);
}

void EpochManager::Protect() {
    epoch_table_.Protect(current_epoch_.load(std::memory_order_relaxed));
}

void EpochManager::Unprotect() {
    epoch_table_.Unprotect();
}

bool EpochManager::IsProtected() {
    return epoch_table_.IsProtected();
}

epoch EpochManager::GetCurrentEpoch() {
    return current_epoch_.load(std::memory_order_relaxed);
}

void EpochManager::SetCurrentEpoch(epoch current_epoch) {
    return current_epoch_.store(current_epoch, std::memory_order_relaxed);
}

bool EpochManager::IsSafeToReclaim(epoch removed_epoch) {
    return removed_epoch < safe_to_reclaim_epoch_.load(std::memory_order_relaxed);
}

void EpochManager::BumpCurrentEpoch() {
    current_epoch_.fetch_add(1, std::memory_order_relaxed);
    ComputeNewSafeToReclaimEpoch(current_epoch_.load(std::memory_order_acq_rel));
}

void EpochManager::ComputeNewSafeToReclaimEpoch(epoch current_epoch) {
    safe_to_reclaim_epoch_.store( \
    epoch_table_.ComputeNewSafeToReclaimEpoch(current_epoch));
}

epoch EpochManager::GetSafeToReclaimEpoch() {
    return safe_to_reclaim_epoch_.load(std::memory_order_relaxed);
}

EpochTable::EpochTable() {
    epoch_entries_ = (EpochEntry*)malloc(sizeof(EpochEntry)*kEpochTableSize);
    for(int i=0; i<kEpochTableSize; i++) {
        new (&epoch_entries_[i]) EpochEntry();
    }
    tail_.store(0, std::memory_order_seq_cst);
}

EpochTable::~EpochTable() {
    if(epoch_entries_ != nullptr)
        free(epoch_entries_);
}

void EpochTable::Protect(epoch current_epoch) {
    EpochEntry* epoch_entry = GetThreadEpochEntry();
    epoch_entry->protected_epoch_.store( \
        current_epoch, std::memory_order_relaxed);
}

void EpochTable::Unprotect() {
    EpochEntry* epoch_entry = GetThreadEpochEntry();
    epoch_entry->protected_epoch_.store( \
        0, std::memory_order_relaxed);
}

bool EpochTable::IsProtected() {
    EpochEntry* epoch_entry = GetThreadEpochEntry();
    return epoch_entry -> protected_epoch_.load( \
        std::memory_order_relaxed) != 0;
}

EpochEntry * EpochTable::GetThreadEpochEntry() {
    thread_local EpochEntry* thread_epoch_entry = nullptr;
    if(thread_epoch_entry == nullptr) {
        unsigned long int slot = tail_.fetch_add(1, std::memory_order_seq_cst);
        thread_epoch_entry = &epoch_entries_[slot];
    }
    return thread_epoch_entry;
}

epoch EpochTable::ComputeNewSafeToReclaimEpoch(epoch current_epoch) {
    unsigned long int curr_tail = tail_.load(std::memory_order_relaxed);
    epoch tmp;
    epoch minimum_epoch = current_epoch;
    for(int i=0; i<curr_tail; i++) {
        tmp = epoch_entries_[i].protected_epoch_.load( \
            std::memory_order_relaxed);
        if(tmp < minimum_epoch && tmp !=0 ) {
            minimum_epoch = tmp;
        }
    }
    return minimum_epoch;
}

EpochEntry::EpochEntry(std::atomic<epoch> protected_epoch) {
    protected_epoch_.store(protected_epoch, std::memory_order_relaxed);
}

EpochEntry::EpochEntry() {
    protected_epoch_.store(0, std::memory_order_relaxed);
}

#endif