/*
 * dudetm is implemented with PL2 not tinySTM.
 */

#ifndef PMDKTX_H_
#define PMDKTX_H_

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#ifndef PERSIST_H_
#include "persist.h"
#endif

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#ifndef THREAD_SAFE_QUEUE_H_
#include "thread_safe_queue.h"
#endif

// #define USE_AEP

#ifdef USE_AEP
#include "aep.h"
#endif

// #define ASYNC_DUDETM

#include <vector>
#include <iostream>
#include <setjmp.h>
#include <mutex>
#include <cstring>
#include <algorithm>
#include <shared_mutex>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>

#define PTM_THREAD_CLEAN do { \
    ; \
}while(0);

#define pr_info_and_exit(str) do { \
    std::cout << str << ", FILE: " << __FILE__ << \
        ", LINE: " << __LINE__ << std::endl; \
    exit(-1); \
}while(0);

#define PTM_START(tx_mode) do { \
    sigjmp_buf *_e = sth_ptm_start(tx_mode); \
    if (_e != NULL) { \
        /* save the current signal mask */ \
        sigsetjmp(*_e, 1); \
    } \
}while(0);

#define PTM_COMMIT do { \
    sth_ptm_commit(); \
}while(0);

class Transaction;

enum TransactionStatus {
    ACTIVE = 0,
    COMMITTED,
    ABORTED,
    UNDETERMINED,
};

enum TransactionMode {
    RDONLY = 0x1,
    RDWR   = 0x2,
};

enum OpenMode {
    READ = 0,
    WRITE,
};

template <typename T>
class PtmObjectWrapper;
class Transaction;
static void sth_ptm_abort();

#define INF ((unsigned long long)(-1L))

/*
 * All ptm objects should inherit this abstract class.
 */
class AbstractPtmObject {
public:
    AbstractPtmObject() {};
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    virtual ~AbstractPtmObject() {};
};

class AbstractPtmObjectWrapper {
public: 
    virtual void CommitWrite() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct WriteSetEntry {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
};

class WriteSet {
public:
    WriteSetEntry *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    WriteSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (WriteSetEntry *)malloc(sizeof(WriteSetEntry) * capacity_);
        memset(set_, 0, sizeof(WriteSetEntry) * capacity_);
    }
    ~WriteSet() {
        Clear();
        if (set_ != nullptr)
            delete set_;
    }
    void Clear() {
        entries_num_ = 0;
    }
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, void *addr, int size) {
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
#ifdef USE_AEP
        char *undolog = (char *)vmem_malloc(size);
#else
        char *undolog = (char *)malloc(size);
#endif
        memcpy(undolog, addr, size);
        clflush((char *)undolog, size);
    }
    bool GetWrtieObjectBy(AbstractPtmObjectWrapper *ptm_object_wrapper) {
        for(int i=0; i<entries_num_; i++) {
            if(set_[i].ptm_object_wrapper_ == ptm_object_wrapper)
                return true;
        }
        return false;
    }
    void CommitWrites() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite();
        }
    }
};

class Transaction {
public:
    TransactionMode     mode_;
    WriteSet            *w_set_;
    sigjmp_buf          *env_;

public:
    Transaction() {
        w_set_ = new WriteSet();
        env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
    }
 
    ~Transaction() {
        if (w_set_ != nullptr)
            delete w_set_;
        if (env_ != nullptr)
            free(env_);
    }
};

// thread things
thread_local Transaction thread_tx;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    PtmObjectWrapper() {};

    T *Open(OpenMode mode) {
        if (mode == READ) {
            return OpenWithRead();
        } else if (mode == WRITE) {
            return OpenWithWrite();
        }
        return nullptr;
    }
    void CommitWrite() {
        clflush((char *)&pm_object_, sizeof(T));
    }
private:
    // pm_object_ is in PM
    T pm_object_;

    // for now, we didn't consider release T*
    T* OpenWithRead() {
        return &pm_object_;
    }

    T *OpenWithWrite() {
retry:
        bool does_exist = (T *)thread_tx.w_set_->GetWrtieObjectBy(this);
        if (does_exist == false) {
            thread_tx.w_set_->Push(this, &pm_object_, sizeof(T));
        }
        return &pm_object_;
    }
};

void InitTransaction() {
    thread_tx.w_set_->Clear();
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
    thread_tx.mode_ = mode;
    InitTransaction();
    return thread_tx.env_;
}

static void sth_ptm_commit() {
    if (thread_tx.mode_ == RDWR) {
        thread_tx.w_set_->CommitWrites();
    }
}

void sth_ptm_abort() {
    thread_abort_counter++;
    InitTransaction();
    siglongjmp(*(thread_tx.env_), 1);
}

#endif