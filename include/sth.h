/*
 * We do not support nested transaction for now.
 */

#ifndef STH_H_
#define STH_H_

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#include <vector>
#include <iostream>
#include <setjmp.h>
#include <mutex>

#define pr_info_and_exit(str) do { \
    std::cout << str << std::endl; \
    exit(-1); \
}while(0);

#define PTM_START do { \
    sigjmp_buf *_e = sth_ptm_start(); \
    if (_e != NULL) { \
        /* save the current signal mask */ \
        sigsetjmp(*_e, 1); \
    } \
}while(0);

#define PTM_COMMIT do { \
    sth_ptm_commit(); \
}while(0);

#define INF ((unsigned long long)(-1L))
unsigned long long global_timestamp = 0;

enum TransactionStatus {
    ACTIVE = 0,
    COMMITTED, 
    ABORTED,
    UNDETERMINED,
};

enum OpenMode {
    READ = 0,
    WRITE,
};

class PtmObject;

struct PtmObjectWithVersionNum {
    PtmObject *ptm_object_;
    unsigned long long version_num_;
};

class Transaction {
public:
    sigjmp_buf env_;
    int status_;
    unsigned long long start_;
    unsigned long long end_;
    bool is_readonly_;
    std::vector<PtmObjectWithVersionNum> r_set_;
    std::vector<PtmObjectWithVersionNum> w_set_; 

    Transaction() {
        status_ = UNDETERMINED;
        is_readonly_ = true;
    }
};

class PtmObject {
public:
    struct ObjectWithVersionNum {
        void *object_;
        unsigned long long version_num_;    // version_num_ can be a timestamp.
    };

public:
    PtmObject(void *object) {
        // I am not clear when generating a new PtmObject
        Transaction *tx;
        tx = GetThreadTransaction();
        if(tx->status_ !=  ACTIVE)
            pr_info_and_exit("there is no active transactions");
        tx_ = tx;
        new_ = object;
        old_ = nullptr;
        version_num_ = 0;
        olders.clear();
    }

    void *Open(OpenMode mode) {
        Transaction *tx;
        tx = GetThreadTransaction();
        if(tx->status_ !=  ACTIVE)
            pr_info_and_exit("there is no active transactions");
        if(mode == READ)
            return OpenWithRead(tx);
        return OpenWithWrite(tx);
    }
private:
    Transaction *tx_;
    void *new_;
    void *old_;
    unsigned long long version_num_;    // version_num_ can be a timestamp.
    std::mutex mutex_;
    // for now, this is not used, we just implement one-version.
    std::vector<ObjectWithVersionNum> olders;

    void *OpenWithRead(Transaction *tx) {
        if(version_num_ < tx->end_) {
            tx->start_ = std::max(tx->start_, version_num_);
            tx->end_ = std::min(tx->end_, ATOMIC_LOAD(&global_timestamp));
            // there exists an concurrent problem
            tx->r_set_.push_back(PtmObjectWithVersionNum{this, version_num_});
        }else {
            sth_ptm_abort(tx);
        }
    }

    void *OpenWithWrite(Transaction *tx) {
        if(tx->is_readonly_ == true)
            tx->is_readonly_ = false;
        if(version_num_ < tx->end_) {
            tx->start_ = std::max(tx->start_, version_num_);
            tx->end_ = std::min(tx->end_, ATOMIC_LOAD(&global_timestamp));
        }else {
            sth_ptm_abort(tx);
        }
    }
};

static inline void sth_ptm_abort(Transaction *tx) {
    tx->status_ = ACTIVE;
    tx->start_ = ATOMIC_LOAD(&global_timestamp);
    tx->end_ = INF;
    tx->is_readonly_ = true;
    tx->r_set_.clear();
    tx->w_set_.clear();
    siglongjmp(tx->env_, 1);
}

static inline void sth_ptm_commit() {
    Transaction *tx;
    tx = GetThreadTransaction();
    if(tx->status_ != ACTIVE)
        pr_info_and_exit("there is no active transactions");
    
}

static inline jmp_buf *sth_ptm_start() {
    Transaction *tx;
    tx = GetThreadTransaction();
    tx->status_ = ACTIVE;
    tx->start_ = ATOMIC_LOAD(&global_timestamp);
    tx->end_ = INF;
    tx->is_readonly_ = true;
    tx->r_set_.clear();
    tx->w_set_.clear();
    return &tx->env_;
}

static inline Transaction *GetThreadTransaction() {
    thread_local Transaction *tx = nullptr;
    if(tx == nullptr)
        tx = new Transaction();
    return tx;
}




#endif