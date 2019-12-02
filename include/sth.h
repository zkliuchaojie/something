/*
 * We do not support nested transaction for now.
 */

#ifndef STH_H_
#define STH_H_

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#ifndef PERSIST_H_
#include "persist.h"
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
#include <cstring>

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
class RwSet;

#define INF ((unsigned long long)(-1L))
volatile unsigned long long global_counter = 0;
thread_local unsigned int thread_counter = 0;
thread_local Transaction *thread_tx = nullptr;
thread_local RwSet *thread_r_set = nullptr;
thread_local RwSet *thread_w_set = nullptr;
thread_local sigjmp_buf *thread_env_ = nullptr;
thread_local unsigned long thread_abort_counter = 0;

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

// class alignas(kCacheLineSize) Transaction {
class Transaction {
public:
    unsigned long long start_;
    RwSet *r_set_;
    RwSet *w_set_;
    sigjmp_buf *env_;
    PtmObjectWrapperWithVersionNum *record_w_set_;
    int record_w_set_num_;
    volatile TransactionStatus status_;
    bool is_readonly_;
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
        r_set_ = nullptr;
        w_set_ = nullptr;
        reference_count_ = 0;
        env_ = nullptr;
    }
 
    ~Transaction() {
        ;
    }
    
    void SetRC(int rc) {
        reference_count_ = rc;
    }

    int DecAndFetchRC() {
        return ATOMIC_SUB_FETCH(&reference_count_, 1);
    }

    bool IsSafeToFree() {
        return ATOMIC_LOAD(&reference_count_) == 0;
    }

    void RecordWriteSet() {
        record_w_set_num_ = w_set_->entries_num_;
        record_w_set_ = (PtmObjectWrapperWithVersionNum *)malloc( \
            record_w_set_num_ * sizeof(PtmObjectWrapperWithVersionNum));
        memcpy(record_w_set_, w_set_->set_, record_w_set_num_ * sizeof(PtmObjectWrapperWithVersionNum));
    }
};

/*
 * All ptm objects should inherit this abstract class.
 */
class AbstractPtmObject {
public:
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    virtual ~AbstractPtmObject() {};
    virtual AbstractPtmObject *Clone() = 0;
    // copy ptm_object to this
    virtual void Copy(AbstractPtmObject *ptm_object) = 0;
    virtual void Free() = 0;
};

struct OldObject {
    unsigned long long version_num_;    // version_num_ can be a timestamp.
    AbstractPtmObject *object_;
    Transaction *belonging_tx_;
};

/*
 * the class name is ugly
 */
class Olders {
public:
    Olders(int size = kOldVersionsDefaultSize) : kSize_(size), pos_(0) {
        old_objects_ = (OldObject*)malloc(sizeof(OldObject)*kSize_);
        memset(old_objects_, 0, sizeof(OldObject)*kSize_);
        if (old_objects_ == nullptr)
            pr_info_and_exit("failed to alloc memory space for old versions");
    }
    ~Olders() {
        if (old_objects_ != nullptr)
            free(old_objects_);
    }
    void Insert(unsigned long long version_num, AbstractPtmObject *curr, \
        Transaction *belonging_tx) {
        /* free object and dec reference counter */
        if (old_objects_[pos_].belonging_tx_ != nullptr && \
            old_objects_[pos_].belonging_tx_->DecAndFetchRC() == 0)
            // it maybe not safe to delet tx directly
            delete old_objects_[pos_].belonging_tx_;
        if (old_objects_[pos_].object_ != nullptr) {
            old_objects_[pos_].object_->Copy(curr);
        } else {
            old_objects_[pos_].object_ = curr->Clone();
        }
        old_objects_[pos_].version_num_ = version_num;
        old_objects_[pos_].belonging_tx_ = belonging_tx;
        pos_ = (pos_ + 1) % kSize_;
    }
private:
    const int kSize_;
    int pos_;   // pointing to a position that we can insert
    OldObject *old_objects_;
};

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
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
        // put current version into olders first
        olders_.Insert(curr_version_num_, &curr_, curr_tx_);
        // set current version info
        // we need a atomic operation to store curr_tx_ and curr_version_num_
        curr_tx_ = tx;
        curr_version_num_ = commit_timestamp;
        /*
         * Since the copy func is not atomic, readers may read partial new data.
         * Therefore, readers must check the POW's transaction status.
         */
        curr_.Copy(new_);
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
    Olders olders_;

    AbstractPtmObject *OpenWithRead(Transaction *tx) {
        // std::cout << "OpenWithRead" << std::endl;
        if (curr_tx_ != nullptr && curr_tx_->status_ != COMMITTED)
            sth_ptm_abort(tx);
        // this overhead is very large
        // if (tx->r_set_->DoesContain(this) == true)
        //     return &curr_;
        tx->r_set_->Push(this, curr_version_num_);
        return &curr_;
    }
    /*
     * OpenWithInit is used to init a new PtmObjectWrapper.
     */
    AbstractPtmObject *OpenWithInit() {
        return &curr_;
    }
    /*
     * The return value won't be seen by other threads, so we
     * do not need to use Pretect/Unprotect in mm_pool.
     */
    AbstractPtmObject *OpenWithWrite(Transaction *tx) {
        //std::cout << "OpenWithWrite" << std::endl;
        if (curr_tx_!=nullptr && curr_tx_->status_ != COMMITTED)
            sth_ptm_abort(tx);
        if(tx->is_readonly_ == true)
            tx->is_readonly_ = false;
        // check if we already put this object into write set
        if (tx->w_set_->DoesContain(this) == true)
            return new_;
        //std::cout << "before try lock" << std::endl;
        if(mutex_.try_lock() == true) {
            // there exists an concurrent bug
            tx->w_set_->Push(this, curr_version_num_);
            if (new_ == nullptr) {
                new_ = curr_.Clone();
            }
            return new_;
        }else {
            sth_ptm_abort(tx);
        }
    }
};

void InitTransaction(Transaction *tx) {
    tx->status_ = ACTIVE;
    tx->start_ = global_counter;
    tx->is_readonly_ = true;
    tx->r_set_->Clear();
    tx->w_set_->Clear();
}

static jmp_buf *sth_ptm_start() {
    Transaction *tx;
    tx = GetThreadTransaction();
    InitTransaction(tx);
    //std::cout << "start ptm" << std::endl;
    return tx->env_;
}

static void sth_ptm_validate(Transaction *tx) {
    if (tx->r_set_->Validate() == false) {
        sth_ptm_abort(tx);
    }
}

/*
 * do not consider durability yet.
 */
static void sth_ptm_commit() {
    Transaction *tx;
    tx = GetThreadTransaction();
    if (tx->status_ != ACTIVE)
        pr_info_and_exit("there is no active transactions");
    if (tx->is_readonly_ == true) {
        tx->status_ = UNDETERMINED;
    } else {
        sth_ptm_validate(tx);
        unsigned long long commit_timestamp;
        commit_timestamp = global_counter + thread_counter;
        thread_counter++;
        if (thread_counter >= kMaxThreadCounter) {
            thread_counter = 0;
            ATOMIC_FETCH_ADD(&global_counter, 1);
        }
        // commit writes
        tx->w_set_->CommitWrites(commit_timestamp, tx);
        tx->w_set_->Unlock();
        tx->SetRC(tx->w_set_->GetEntriesNum());
        tx->RecordWriteSet();
        mfence(); // we need a mfence here
        tx->status_ = COMMITTED;
    }
}

static void sth_ptm_abort(Transaction *tx) {
    tx->status_ = ACTIVE;
    tx->start_ = global_counter;
    tx->is_readonly_ = true;
    tx->w_set_->FreeNew();
    tx->w_set_->Unlock();
    tx->r_set_->Clear();
    tx->w_set_->Clear();
    thread_abort_counter++;
    siglongjmp(*(tx->env_), 1);
}

static Transaction *GetThreadTransaction() {
    if (thread_tx == nullptr || thread_tx->status_ == COMMITTED) {
        if (thread_r_set == nullptr)
            thread_r_set = new RwSet();
        if (thread_w_set == nullptr)
            thread_w_set = new RwSet();
        if (thread_env_ == nullptr)
            thread_env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
        thread_tx = new Transaction();
        thread_tx->r_set_ = thread_r_set;
        thread_tx->w_set_ = thread_w_set;
        thread_tx->env_ = thread_env_;
    }
    return thread_tx;
}

#endif