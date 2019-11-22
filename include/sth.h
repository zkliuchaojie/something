/*
 * We do not support nested transaction for now.
 */

#ifndef STH_H_
#define STH_H_

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#ifndef MM_POOL_H_
#include "mm_pool.h"
#endif

#ifndef MM_ABSTRACT_OBJECT_H_
#include "mm_abstract_object.h"
#endif

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#include <vector>
#include <iostream>
#include <setjmp.h>
#include <mutex>

#define pr_info_and_exit(str) do { \
    std::cout << str << ", FILE: " << __FILE__ << \
        ", LINE: " << __LINE__ << std::endl; \
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
volatile unsigned long long global_timestamp = 0;

enum TransactionStatus {
    ACTIVE = 0,
    COMMITTED, 
    ABORTED,
    UNDETERMINED,
};

enum OpenMode {
    READ = 0,
    WRITE,
    INIT,
};

template <typename T>
class PtmObjectWrapper;
class Transaction;
static Transaction *GetThreadTransaction();
static void sth_ptm_abort(Transaction *tx);

class AbstractPtmObjectWrapper {
public:
    virtual void FreeNew() = 0;
    virtual bool Validate(unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp, Transaction *tx) = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct PtmObjectWrapperWithVersionNum {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    unsigned long long version_num_;
};

class RwSet {
public:
    PtmObjectWrapperWithVersionNum *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    RwSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (PtmObjectWrapperWithVersionNum *)malloc( \
            sizeof(PtmObjectWrapperWithVersionNum) * capacity_);
    }
    ~RwSet() {
        delete set_;
    }
    unsigned long long GetEntriesNum() {
        return entries_num_;
    }
    void Clear() {
        entries_num_ = 0;
    }
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, unsigned long long version_num) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].version_num_ = version_num;
        entries_num_++;
    }
    bool DoesContain(AbstractPtmObjectWrapper *ptm_object_wrapper) {
        for(int i=0; i<entries_num_; i++) {
            if(set_[i].ptm_object_wrapper_ == ptm_object_wrapper)
                return true;
        }
        return false;
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].version_num_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long commit_timestamp, Transaction *tx) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(commit_timestamp, tx);    
        }
    }
    void Unlock() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Unlock(); 
        }
    }
    void FreeNew() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->FreeNew();   
        }
    }

};

class alignas(kCacheLineSize) Transaction : public MMAbstractObject {
public:
    volatile TransactionStatus status_;
    unsigned long long start_;
    unsigned long long end_;
    bool is_readonly_;
    RwSet *r_set_;
    RwSet *w_set_;
    sigjmp_buf env_;
public:
    /* 
     * recording how many objects refers this tx, if is 0, 
     * then we can free this tx.
     */
    int reference_count_;   

public:
    Transaction() {
        status_ = UNDETERMINED;
        is_readonly_ = true;
        r_set_ = new RwSet();
        w_set_ = new RwSet();
        reference_count_ = 0;
    }
 
    ~Transaction() {
        delete r_set_;
        delete w_set_;
    }
    
    void SetRC(int rc) {
        reference_count_ = rc;
    }

    void DecRC() {
        reference_count_--;
    }

    bool IsSafeToFree() {
        return reference_count_ == 0;
    }
};

/*
 * All ptm objects should inherit this abstract class.
 */
class AbstractPtmObject : public MMAbstractObject {
public:
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    virtual ~AbstractPtmObject() {};
    virtual AbstractPtmObject *Clone() = 0;
    // copy ptm_object to this
    virtual void Copy(AbstractPtmObject *ptm_object) = 0;
    virtual void Free() = 0;
};

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    struct ObjectWithVersionNum {
        unsigned long long version_num_;    // version_num_ can be a timestamp.
        void *object_;
        Transaction *belonging_tx_;
    };

public:
    PtmObjectWrapper() {
        // I am not clear when generating a new PtmObject
        Transaction *tx;
        tx = GetThreadTransaction();
        if(tx->status_ !=  ACTIVE)
            pr_info_and_exit("there is no active transactions");
        curr_tx_ = nullptr;     // init with nullptr
        curr_version_num_ = 0;  // version num starts from 0
        new (&curr_) T();
        new_ = nullptr;
        olders.clear();
    }

    AbstractPtmObject *Open(OpenMode mode) {
        //std::cout << "Open" << std::endl;
        Transaction *tx;
        tx = GetThreadTransaction();
        if(tx->status_ !=  ACTIVE)
            pr_info_and_exit("there is no active transactions");
        if(mode == READ) {
            //std::cout << "before OpenWithRead" << std::endl;
            return OpenWithRead(tx);
        }
        else if (mode == WRITE) {
            //std::cout << "before OpenWithWrite" << std::endl;
            return OpenWithWrite(tx);
        }
        else if (mode == INIT)
            return OpenWithInit();
    }

    void FreeNew() {
        new_->Free();
        new_ = nullptr;
    }
    bool Validate(unsigned long long version_num) {
        if (curr_version_num_ != version_num)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long commit_timestamp, Transaction *tx) {
        curr_tx_ = tx;
        //mfence // we need a mfence
        curr_version_num_ = commit_timestamp;
        /*
         * Since the copy func is not atomic, readers may read partial new data.
         * Therefore, readers must check the POW's transaction status.
         */
        curr_.Copy(new_);
        new_->Free();
        new_ = nullptr;
    }
    void Unlock() {
        mutex_.unlock();
    }

private:
    Transaction *curr_tx_;
    unsigned long long curr_version_num_;    // version_num_ can be a timestamp.
    T curr_;
    AbstractPtmObject *new_;
    std::mutex mutex_;
    // for now, this is not used, we just implement one-version.
    std::vector<ObjectWithVersionNum> olders;

    AbstractPtmObject *OpenWithRead(Transaction *tx) {
        //std::cout << "OpenWithRead" << std::endl;
        if (curr_tx_ != nullptr && curr_tx_->status_ != COMMITTED)
            sth_ptm_abort(tx);
        // we do not need to check if this object already exists in r_set_.
        if (curr_version_num_ <= tx->end_) {
            tx->start_ = std::max(tx->start_, curr_version_num_);
            // the global_timestamp don't need to be very precise.
            tx->end_ = tx->end_ > global_timestamp ? \
                global_timestamp : tx->end_;
            // there exists an concurrent bug
            tx->r_set_->Push(this, curr_version_num_);
            return &curr_;
        }else {
            sth_ptm_abort(tx);
        }
    }
    /*
     * OpenWithInit is used to init a new PtmObjectWrapper.
     */
    AbstractPtmObject *OpenWithInit() {
        return &curr_;
    }

    AbstractPtmObject *OpenWithWrite(Transaction *tx) {
        //std::cout << "OpenWithWrite" << std::endl;
        if (curr_tx_!=nullptr && curr_tx_->status_ != COMMITTED)
            sth_ptm_abort(tx);
        if(tx->is_readonly_ == true)
            tx->is_readonly_ = false;
        // check if we already put this object into write set
        if (tx->w_set_->DoesContain(this) == true)
            return new_;
        if(curr_version_num_ <= tx->end_) {
            tx->start_ = std::max(tx->start_, curr_version_num_);
            tx->end_ = tx->end_ > global_timestamp ? \
                global_timestamp : tx->end_;
            //std::cout << "before try lock" << std::endl;
            if(mutex_.try_lock() == true) {
                // there exists an concurrent bug
                tx->w_set_->Push(this, curr_version_num_);
                new_ = curr_.Clone();
                return new_;
            }else {
                sth_ptm_abort(tx);
            }
        }else {
            sth_ptm_abort(tx);
        }
    }
};

static jmp_buf *sth_ptm_start() {
    Transaction *tx;
    tx = GetThreadTransaction();
    tx->status_ = ACTIVE;
    tx->start_ = ATOMIC_LOAD(&global_timestamp);
    tx->end_ = INF;
    tx->is_readonly_ = true;
    tx->r_set_->Clear();
    tx->w_set_->Clear();
    //std::cout << "start ptm" << std::endl;
    return &tx->env_;
}

static void sth_ptm_validate(Transaction *tx) {
    if (tx->r_set_->Validate() == false)
        sth_ptm_abort(tx);
}

static void sth_ptm_commit() {
    Transaction *tx;
    tx = GetThreadTransaction();
    if (tx->status_ != ACTIVE)
        pr_info_and_exit("there is no active transactions");
    if (tx->is_readonly_ == false) {
        sth_ptm_validate(tx);
        unsigned long long commit_timestamp;
        commit_timestamp = ATOMIC_FETCH_ADD(&global_timestamp, 1);
        // commit writes
        tx->w_set_->CommitWrites(commit_timestamp, tx);
        tx->w_set_->Unlock();
    }
    tx->SetRC(tx->w_set_->GetEntriesNum());
    //mfence(); // we need a mfence here
    tx->status_ = COMMITTED;
}

static void sth_ptm_abort(Transaction *tx) {
    tx->status_ = ACTIVE;
    tx->start_ = ATOMIC_LOAD(&global_timestamp);
    tx->end_ = INF;
    tx->is_readonly_ = true;
    tx->w_set_->FreeNew();
    tx->w_set_->Unlock();
    tx->r_set_->Clear();
    tx->w_set_->Clear();
    siglongjmp(tx->env_, 1);
}

static Transaction *GetThreadTransaction() {
    thread_local Transaction *thread_tx = nullptr;
    if (thread_tx == nullptr ||  thread_tx->status_ == COMMITTED)
        thread_tx = new Transaction();
    return thread_tx;
}

#endif