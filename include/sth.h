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
#include <algorithm>
#include <shared_mutex>

#define TI_AND_TS(ti, ts)   (((unsigned long long)ti<<(sizeof(unsigned long long)*8-kThreadIdBitSize)) | (ts))
#define TS(ti_and_ts)       ((ti_and_ts << kThreadIdBitSize) >> kThreadIdBitSize)
#define TI(ti_and_ts)       (ti_and_ts >> (sizeof(unsigned long long)*8-kThreadIdBitSize))
#define MAX(v1, v2)         (v1>v2 ? v1 : v2)
#define MIN(v1, v2)         (v1>v2 ? v2 : v1)

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

enum TransactionStatus {
    ACTIVE = 0,
    COMMITTED, 
    ABORTED,
    UNDETERMINED,
};

enum TransactionMode {
    RDONLY = 0,
    RDWR
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
// global things
volatile int thread_id_allocator = 0;
volatile unsigned long long thread_timestamps[kMaxThreadNum]={0};

// thread things
thread_local Transaction *thread_tx = nullptr;
thread_local int thread_id = -1;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;

class AbstractPtmObjectWrapper {
public:
    virtual void FreeNew() = 0;
    virtual bool Validate(unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp, TransactionStatus *tx_status) = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct PtmObjectWrapperWithVersionNum {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    unsigned long long ti_and_ts_;
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
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, unsigned long long ti_and_ts) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ti_and_ts_ = ti_and_ts;
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
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ti_and_ts_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long ti_and_ts, TransactionStatus *tx_status) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ti_and_ts, tx_status);
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
    /*
     * starts_ are used to record all threads' starting time,
     * which is similar to ends_.
     */
    unsigned long long *starts_, *ends_;
    unsigned long long objects_biggest_ts_;
    RwSet *r_set_;
    RwSet *w_set_;
    sigjmp_buf *env_;
    volatile TransactionStatus status_;
    TransactionMode mode_;

public:
    Transaction() {
        status_ = UNDETERMINED;
        mode_ = RDWR;
        starts_ = new unsigned long long[kMaxThreadNum];
        ends_ = new unsigned long long[kMaxThreadNum];
        r_set_ = new RwSet();
        w_set_ = new RwSet();
        env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
    }
 
    ~Transaction() {
        if (starts_ != nullptr)
            delete [] starts_;
        if (ends_ != nullptr)
            delete [] ends_;
        if (r_set_ != nullptr)
            delete r_set_;
        if (w_set_ != nullptr)
            delete w_set_;
        if (env_ != nullptr)
            free(env_);
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
    unsigned long long ti_and_ts_;    // version_num_ can be a timestamp.
    AbstractPtmObject *object_;
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
    void Insert(unsigned long long ti_and_ts, AbstractPtmObject *curr) {
        shared_mutex_.lock();
        /*
         * be carefull, other threads may use the old object that
         * we will overwrite, so we need reference count to check it.
         */
        if (old_objects_[pos_].object_ != nullptr) {
            old_objects_[pos_].object_->Copy(curr);
        } else {
            old_objects_[pos_].object_ = curr->Clone();
        }
        old_objects_[pos_].ti_and_ts_ = ti_and_ts;
        pos_ = (pos_ + 1) % kSize_;
        shared_mutex_.unlock();
    }
    OldObject *Search(unsigned long long *starts_, unsigned long long *ends_) {
        shared_mutex_.lock_shared();
        OldObject *old_object;
        for (int i=0; i<kSize_; i++) {
            old_object = &old_objects_[(pos_+i)%kSize_];
            if (old_object->object_ == nullptr) {
                shared_mutex_.unlock_shared();
                return nullptr;
            }
            if (TS(old_object->ti_and_ts_) <= ends_[TI(old_object->ti_and_ts_)]) {
                shared_mutex_.unlock_shared();
                return old_object;
            } else {
                // we should minus 1
                ends_[TI(old_object->ti_and_ts_)] = \
                    MIN(ends_[TI(old_object->ti_and_ts_)], TS(old_object->ti_and_ts_)-1);
            }
        }
        shared_mutex_.unlock_shared();
        return nullptr;
    }
private:
    const int kSize_;
    int pos_;   // pointing to a position that we can insert
    OldObject *old_objects_;
    std::shared_mutex shared_mutex_;
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
        curr_tx_status_ = nullptr;     // init with nullptr
        curr_ti_and_ts_ = 0;  // version num starts from 0
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
    bool Validate(unsigned long long ti_and_ts) {
        if (curr_ti_and_ts_ != ti_and_ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ti_and_ts, TransactionStatus *tx_status) {
        // put current version into olders first
        olders_.Insert(curr_ti_and_ts_, &curr_);
        // set current version info
        // we need a atomic operation to store curr_tx_ and curr_version_num_
        curr_tx_status_ = tx_status;
        curr_ti_and_ts_ = ti_and_ts;
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
    volatile TransactionStatus *curr_tx_status_;
    // ti means thread id, ts means timestamp
    unsigned long long curr_ti_and_ts_;    // version_num_ can be a timestamp.
    T curr_;
    AbstractPtmObject *new_;
    std::mutex mutex_;
    // for now, this is not used, we just implement one-version.
    Olders olders_;

    /*
     * when OpenWithRead, it may be a Update Tx, but we don't know
     * until OpenWithWrite. So we will use old objects, and there is
     * a bug. For example, for a linkedlist, when deleting an object,
     * you search it first. In this process, if you use old objects,
     * you may can't find it, then the delete operation failed. So we
     * need users tell us whether it is a update transaction.
     */
    AbstractPtmObject *OpenWithRead(Transaction *tx) {
        // std::cout << "OpenWithRead" << std::endl;
        if ((curr_tx_status_ == nullptr || *curr_tx_status_ == COMMITTED) \
            && (TS(curr_ti_and_ts_) <= tx->ends_[TI(curr_ti_and_ts_)])) {
            tx->starts_[TI(curr_ti_and_ts_)] = \
                MAX(tx->starts_[TI(curr_ti_and_ts_)], TS(curr_ti_and_ts_));
            tx->ends_[TI(curr_ti_and_ts_)] = \
                MIN(tx->ends_[TI(curr_ti_and_ts_)], thread_timestamps[TI(curr_ti_and_ts_)]);
            // this overhead is very large
            // if (tx->r_set_->DoesContain(this) == true)
            //     return &curr_;
            tx->r_set_->Push(this, curr_ti_and_ts_);
            return &curr_;
        } else if (tx->mode_ == RDONLY) {
            OldObject *old_object;
            old_object = olders_.Search(tx->starts_, tx->ends_);
            if (old_object != nullptr) {
                tx->starts_[TI(old_object->ti_and_ts_)] = \
                    MAX(tx->starts_[TI(old_object->ti_and_ts_)], TS(old_object->ti_and_ts_));
                tx->r_set_->Push(this, old_object->ti_and_ts_);
                return old_object->object_;
            }
        }
        // std::cout << "thread_id: " << thread_id << std::endl;
        // std::cout << "thread id: " << TI(curr_ti_and_ts_) << std::endl;
        // std::cout << thread_timestamps[TI(curr_ti_and_ts_)] << std::endl;
        // std::cout << TS(curr_ti_and_ts_) << std::endl;
        // std::cout << tx->ends_[TI(curr_ti_and_ts_)] << std::endl;
        if (tx->mode_ == RDONLY)
            thread_read_abort_counter++;
        sth_ptm_abort(tx);
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
        // std::cout << "OpenWithWrite" << std::endl;
        if ((curr_tx_status_!=nullptr && *curr_tx_status_ != COMMITTED)) {
            sth_ptm_abort(tx);
        }
        if (TS(curr_ti_and_ts_) <= tx->ends_[TI(curr_ti_and_ts_)]) {
            // check if we already put this object into write set
            if (tx->w_set_->DoesContain(this) == true)
                return new_;
            //std::cout << "before try lock" << std::endl;
            if(mutex_.try_lock() == true) {
                // update tx.starts_ and tx.ends_
                tx->starts_[TI(curr_ti_and_ts_)] = \
                    MAX(tx->starts_[TI(curr_ti_and_ts_)], TS(curr_ti_and_ts_));
                tx->ends_[TI(curr_ti_and_ts_)] = \
                    MIN(tx->ends_[TI(curr_ti_and_ts_)], thread_timestamps[TI(curr_ti_and_ts_)]);
                // there exists an concurrent bug
                tx->w_set_->Push(this, curr_ti_and_ts_);
                if (TS(curr_ti_and_ts_) > tx->objects_biggest_ts_)
                    tx->objects_biggest_ts_ = TS(curr_ti_and_ts_);
                if (new_ == nullptr)
                    new_ = curr_.Clone();
                return new_;
            }
        }
        sth_ptm_abort(tx);
    }
};

void InitTransaction(Transaction *tx) {
    for (int i=0; i<kMaxThreadNum; i++) {
        tx->starts_[i] = thread_timestamps[i];
        tx->ends_[i] = INF;
    }
    tx->objects_biggest_ts_ = 0;
    tx->status_ = ACTIVE;
    tx->r_set_->Clear();
    tx->w_set_->Clear();
}

static void sth_ptm_validate(Transaction *tx) {
    // std::cout << tx->does_use_olders_ << std::endl;
    if (tx->r_set_->Validate() == false) {
        sth_ptm_abort(tx);
    }
}

static jmp_buf *sth_ptm_start(TransactionMode tx_mode) {
    Transaction *tx;
    tx = GetThreadTransaction();
    tx->mode_ = tx_mode;
    InitTransaction(tx);
    //std::cout << "start ptm" << std::endl;
    return tx->env_;
}

/*
 * do not consider durability yet.
 */
static void sth_ptm_commit() {
    Transaction *tx;
    tx = GetThreadTransaction();
    if (tx->status_ != ACTIVE)
        pr_info_and_exit("there is no active transactions");
    if (tx->mode_ == RDWR) {
        sth_ptm_validate(tx);
        unsigned long long commit_ts = thread_timestamps[thread_id];
        if (tx->objects_biggest_ts_ >= commit_ts)
            commit_ts = tx->objects_biggest_ts_ + 1;
        thread_timestamps[thread_id] = commit_ts + 1;
        //std::cout << "commit: " << thread_id << ", " << thread_timestamps[thread_id] << std::endl;
        // commit writes
        TransactionStatus *tx_status = new TransactionStatus;
        *tx_status = tx->status_;
        tx->w_set_->CommitWrites(TI_AND_TS(thread_id, commit_ts), tx_status);
        tx->w_set_->Unlock();
        mfence(); // we need a mfence here
        *tx_status = tx->status_ = COMMITTED;
    }
}

static void sth_ptm_abort(Transaction *tx) {
    tx->w_set_->FreeNew();
    tx->w_set_->Unlock();
    InitTransaction(tx);
    thread_abort_counter++;
    siglongjmp(*(tx->env_), 1);
}

static Transaction *GetThreadTransaction() {
    if (thread_id == -1) {
        thread_id = ATOMIC_FETCH_ADD(&thread_id_allocator, 1);
        // std::cout << "thread_id: " << thread_id << std::endl;
    }
    if (thread_tx == nullptr) {
        thread_tx = new Transaction();
    }
    return thread_tx;
}

#endif