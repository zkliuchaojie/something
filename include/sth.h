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

// timestamp things
#define TI_AND_TS(ti, ts)   (((unsigned long long)ti<<(sizeof(unsigned long long)*8-kThreadIdBitSize)) | (ts))
#define TS(ti_and_ts)       ((ti_and_ts << kThreadIdBitSize) >> kThreadIdBitSize)
#define TI(ti_and_ts)       (ti_and_ts >> (sizeof(unsigned long long)*8-kThreadIdBitSize))
#define MAX(v1, v2)         (v1>v2 ? v1 : v2)
#define MIN(v1, v2)         (v1>v2 ? v2 : v1)

// transaction mode things
#define MODE_MARK               (0x7)
#define MODE(tx)                ((unsigned long long)tx & MODE_MARK)
#define TX(tx_and_mode)         ((Transaction *)((unsigned long long)tx_and_mode & (~MODE_MARK)))
#define TX_AND_MODE(tx, mode)   ((Transaction *)((unsigned long long)tx | mode))

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

// #define PTM_START(tx_mode)      \
//     sth_ptm_start(tx_mode);     \
// __sth_ptm_start:                \
//     try {                       \

#define PTM_COMMIT do { \
    sth_ptm_commit(); \
}while(0);

#define PTM_THREAD_CLEAN do { \
    sth_ptm_clean_thread(); \
}while(0);

class Transaction;
void sth_ptm_validate(Transaction *tx);

// #define PTM_COMMIT              \
//     sth_ptm_commit();           \
//     } catch(TransactionExceptionAbort e) {  \
//         goto __sth_ptm_start;   \
//     }                           \

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

// transaction exception types
struct TransactionExceptionAbort {};

enum OpenMode {
    READ = 0,
    WRITE,
    INIT,
};

template <typename T>
class PtmObjectWrapper;
class Transaction;
static void sth_ptm_abort();
class RwSet;

#define INF ((unsigned long long)(-1L))
// global things
volatile int thread_id_allocator = 0;
volatile unsigned long long thread_clocks[kMaxThreadNum];     //default 1
volatile unsigned long long thread_epoch_min[kMaxThreadNum];  // defualt 0
volatile unsigned long long thread_epoch[kMaxThreadNum][kMaxThreadNum]; // default INF

// thread things
thread_local Transaction *thread_tx_and_mode = nullptr;
thread_local int thread_id = -1;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;

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
};

class AbstractPtmObjectWrapper {
public: 
    virtual void FreeNew() = 0;
    virtual void ResetCurrTxStatus() = 0;
    virtual bool Validate(int pos, unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp, TransactionStatus *tx_status) = 0;
    virtual void IncPos() = 0;
    virtual void Persist() = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct PtmObjectWrapperWithVersionNum {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    int pos_;
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
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, int pos, unsigned long long ti_and_ts) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].pos_ = pos;
        set_[entries_num_].ti_and_ts_ = ti_and_ts;
        entries_num_++;
        if(entries_num_ > capacity_)
            std::cout << "overflow" << std::endl;
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
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].pos_, set_[i].ti_and_ts_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long ti_and_ts, TransactionStatus *tx_status) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ti_and_ts, tx_status);
        }
    }
    void IncPos() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->IncPos(); 
        }
    }
    void Persist() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Persist();
        }
    }
    void FreeNew() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->FreeNew();
        }
    }
    void ResetCurrTxStatus() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->ResetCurrTxStatus();
        }
    }
    void Unlock() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Unlock(); 
        }
    }
};

class Transaction  {
public:
    /*
     * ends_ are used to record all threads' current time, and all
     * objects' commit time should be less or equal it.
     */
    unsigned long long ends_[kMaxThreadNum];
    unsigned long long latest_[kMaxThreadNum];
    RwSet *r_set_;
    RwSet *w_set_;
    sigjmp_buf *env_;
    volatile TransactionStatus status_;
    int delay_to_read_clocks_counter_;

public:
    Transaction() {
        thread_clocks[thread_id] = 1;
        memset(ends_, 0, sizeof(unsigned long long)*kMaxThreadNum);
        memset(latest_, 0, sizeof(unsigned long long)*kMaxThreadNum);
        status_ = UNDETERMINED;
        delay_to_read_clocks_counter_ = 0;
        r_set_ = new RwSet();
        w_set_ = new RwSet();
        env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
    }
 
    ~Transaction() {
        if (r_set_ != nullptr)
            delete r_set_;
        if (w_set_ != nullptr)
            delete w_set_;
        if (env_ != nullptr)
            free(env_);
    }
};

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    PtmObjectWrapper() {
        // I am not clear when generating a new PtmObject
        for(int i=0; i<kVersionSize; i++)
            objs_[i].ti_and_ts_ = kInvalidTiAndTs;
        pos_ = 0;
        new_ = nullptr;
        lock_ = 0;
        curr_tx_status_ = nullptr;
    }

    T *Open(OpenMode mode) {
        if (mode == READ) {
            return OpenWithRead();
        } else if (mode == WRITE) {
            return OpenWithWrite();
        }
        else if (mode == INIT)
            return OpenWithInit();
    }

    void FreeNew() {
        if (new_ != NULL)
            delete new_;
        new_ = nullptr;
    }
    void ResetCurrTxStatus() {
        if(curr_tx_status_ != nullptr && *curr_tx_status_ != COMMITTED) {
            *curr_tx_status_ = COMMITTED;
        }
    }
    bool Validate(int pos, unsigned long long ti_and_ts) {
        if (pos != (kVersionSize+pos_-1)%kVersionSize \
            || objs_[pos].ti_and_ts_ != ti_and_ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ti_and_ts, TransactionStatus *tx_status) {
        if (objs_[pos_].ti_and_ts_ == kInvalidTiAndTs) {
            // yes, we can overwrite this version
            /*
            * Since the copy func is not atomic, readers may read partial new data.
            * Therefore, readers must check the POW's transaction status.
            */
            curr_tx_status_ = tx_status;
            mfence();
            objs_[pos_].ti_and_ts_ = ti_and_ts;
            objs_[pos_].object_.Copy(new_);
        } else {
            int old_id = TI(objs_[(pos_+1)%kVersionSize].ti_and_ts_);
            unsigned long long old_ts = TS(objs_[(pos_+1)%kVersionSize].ti_and_ts_);
            // the first condition is a must, because objs may be written by aborted tx.
            if (objs_[(pos_+1)%kVersionSize].ti_and_ts_ == kInvalidTiAndTs \
                || old_ts <= thread_epoch_min[old_id]) {
                curr_tx_status_ = tx_status;
                mfence();
                objs_[pos_].ti_and_ts_ = ti_and_ts;
                objs_[pos_].object_.Copy(new_);
            } else {
                // update thread_epoch_min
                unsigned long long mins[thread_id_allocator];
                for(int i=0; i<thread_id_allocator; i++)
                    mins[i] = INF;
                Transaction *tx = TX(thread_tx_and_mode);
                for(int i=0; i<thread_id_allocator; i++) {
                    for(int j=0; j<thread_id_allocator; j++) {
                        if (thread_epoch[i][j] < mins[j]) {
                            mins[j] = thread_epoch[i][j];
                        }
                    }
                }
                for(int i=0; i<thread_id_allocator; i++) {
                    if(mins[i] > thread_epoch_min[i]) {
                        thread_epoch_min[i] = mins[i];
                    }
                }
                sth_ptm_abort();
            }
        }
    }
    void IncPos() {
        pos_ = (pos_+1)%kVersionSize;
    }
    void Persist() {
        clflush((char *)&curr_tx_status_, sizeof(TransactionStatus));
        clflush((char *)&pos_, sizeof(int));
        clflush((char *)&objs_[pos_], sizeof(OldObject));
    }
    void Unlock() {
        // lock_ = 0 is enough
        lock_ = 0;
    }
private:
    volatile TransactionStatus *curr_tx_status_;
    // pos_ is a index, that points the empty space that is used to store the latest version.
    volatile int pos_;
    T *new_;
    int lock_;
    struct OldObject {
        volatile unsigned long long ti_and_ts_;
        T object_;
    } objs_[kVersionSize];
    // std::mutex mutex_;
    /*
     * OpenWithInit is used to init a new PtmObjectWrapper.
     * Be sure that when initializing, no one can access it.
     */
    T *OpenWithInit() {
        curr_tx_status_ = nullptr;
        objs_[pos_].ti_and_ts_ = TI_AND_TS(0, 1);
        return &(objs_[pos_++].object_);
    }
    /*
     * when OpenWithRead, it may be a Update Tx, but we don't know
     * until OpenWithWrite. So we will use old objects, and there is
     * a bug. For example, for a linkedlist, when deleting an object,
     * you search it first. In this process, if you use old objects,
     * you may can't find it, then the delete operation failed. So we
     * need users tell us whether it is a update transaction.
     */
    T* OpenWithRead() {
        Transaction *tx = TX(thread_tx_and_mode);
        // pos_ maybe modifed by other tx, so we first get it, and if it is changed,
        // in OpenWithRead, which won't cause unconsitency. 
        int curr_pos = (kVersionSize+pos_-1)%kVersionSize;
        unsigned long long curr_ti_and_ts = objs_[curr_pos].ti_and_ts_;
        mfence();
        if (curr_tx_status_ == nullptr || *curr_tx_status_ == COMMITTED) {
            if (TS(curr_ti_and_ts) > tx->latest_[TI(curr_ti_and_ts)])
                tx->latest_[TI(curr_ti_and_ts)] = TS(curr_ti_and_ts);
            if((MODE(thread_tx_and_mode) == RDWR) && TS(curr_ti_and_ts)>tx->ends_[TI(curr_ti_and_ts)]) {
                sth_ptm_validate(tx);
                tx->ends_[TI(curr_ti_and_ts)] = TS(curr_ti_and_ts);
            }
            if (curr_tx_status_ == nullptr || TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)]) {
                if(MODE(thread_tx_and_mode) == RDWR)
                    tx->r_set_->Push(this, curr_pos, curr_ti_and_ts);
                return &(objs_[curr_pos].object_);
            }
        } else if (MODE(thread_tx_and_mode) == RDONLY) {
            for(int i=1; i<kVersionSize; i++) {
                curr_pos = (kVersionSize+curr_pos-1)%kVersionSize;
                curr_ti_and_ts = objs_[curr_pos].ti_and_ts_;
                if(curr_ti_and_ts == kInvalidTiAndTs)
                    break;
                if(TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)]) {
                    return &(objs_[curr_pos].object_);
                }
            }
        }
        if (MODE(thread_tx_and_mode) == RDONLY)
            thread_read_abort_counter++;
        sth_ptm_abort();
    }

    /*
     * The return value won't be seen by other threads, so we
     * do not need to use Pretect/Unprotect in mm_pool.
     */
    T *OpenWithWrite() { 
        Transaction *tx = TX(thread_tx_and_mode);
        int curr_pos = (kVersionSize+pos_-1)%kVersionSize;
        unsigned long long curr_ti_and_ts = objs_[curr_pos].ti_and_ts_; 
        mfence();
        if (curr_tx_status_ == nullptr || *curr_tx_status_ == COMMITTED) {
            if (TS(curr_ti_and_ts) > tx->latest_[TI(curr_ti_and_ts)])
                tx->latest_[TI(curr_ti_and_ts)] = TS(curr_ti_and_ts);
            if(TS(curr_ti_and_ts)>tx->ends_[TI(curr_ti_and_ts)]) {
                sth_ptm_validate(tx);
                tx->ends_[TI(curr_ti_and_ts)] = TS(curr_ti_and_ts);
            }
            if (curr_tx_status_ == nullptr || TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)]) {
                if (tx->w_set_->DoesContain(this) == true)
                    return new_;
                int tmp = 0;
                if(CAS(&lock_, &tmp, 1) == true) {
                    if(curr_pos != ((kVersionSize+pos_-1)%kVersionSize) \
                        || curr_ti_and_ts != objs_[curr_pos].ti_and_ts_) {
                        tmp = 1;
                        CAS(&lock_, &tmp, 0);
                        sth_ptm_abort();
                    }
                    tx->w_set_->Push(this, curr_pos, curr_ti_and_ts);
                    if (new_ == nullptr)
                        new_ = (T *)objs_[curr_pos].object_.Clone();
                    return new_;
                }
            }
        }
        sth_ptm_abort();
    }
};

void InitTransaction(Transaction *tx) {
    /*
     * we need to update thread_epoch periodically, even
     * there is no tx to execute, or the CommitWrite may
     * be blocked.
     */
    if (tx->delay_to_read_clocks_counter_ == 0) {
        for(int i=0; i<kMaxThreadNum; i++) {
            tx->ends_[i] = tx->latest_[i];
            thread_epoch[thread_id][i] = tx->ends_[i];
        }
        tx->delay_to_read_clocks_counter_++;
    } else {
        for(int i=0; i<kMaxThreadNum; i++) {
            tx->ends_[i] = thread_clocks[i];
            thread_epoch[thread_id][i] = tx->ends_[i];
        }
        tx->delay_to_read_clocks_counter_++;
        if(tx->delay_to_read_clocks_counter_ == 100)
            tx->delay_to_read_clocks_counter_ = 0;
    }
    tx->status_ = ACTIVE;
    tx->r_set_->Clear();
    tx->w_set_->Clear();
}

void sth_ptm_validate(Transaction *tx) {
    if (tx->r_set_->Validate() == false) {
        sth_ptm_abort();
    }
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
    static int does_init = 0;
    if (!does_init) {
        for(int i=0; i<kMaxThreadNum; i++) {
            thread_epoch_min[i] = 0;
            for(int j=0; j<kMaxThreadNum; j++) {
                thread_epoch[i][j] = INF;
            }
        }
        does_init = 1;
    }
    if (thread_id == -1) {
        thread_id = ATOMIC_FETCH_ADD(&thread_id_allocator, 1);
    }
    Transaction *tx;
    if (thread_tx_and_mode == nullptr)
        tx = new Transaction();
    else
        tx = TX(thread_tx_and_mode);
    thread_tx_and_mode = TX_AND_MODE(tx, mode);
    InitTransaction(tx);
    return tx->env_;
}

/*
 * do not consider durability yet.
 */
static void sth_ptm_commit() {
    Transaction *tx = TX(thread_tx_and_mode);
    if (MODE(thread_tx_and_mode) == RDWR) {
        sth_ptm_validate(tx);
        unsigned long long commit_ts = thread_clocks[thread_id] + 1;
        // commit writes
        TransactionStatus *tx_status = new TransactionStatus;
        *tx_status = tx->status_;
        tx->w_set_->CommitWrites(TI_AND_TS(thread_id, commit_ts), tx_status);
        tx->w_set_->IncPos();
        tx->w_set_->Persist();
        tx->w_set_->Unlock();
        mfence();
        *tx_status = tx->status_ = COMMITTED;
        // be careful, we may loss this memory space
        clflush((char *)tx_status, sizeof(TransactionStatus));
        mfence();
        thread_clocks[thread_id] = commit_ts;
    }
    // update thread_epoch
    for(int i=0; i<kMaxThreadNum; i++) {
        if (tx->latest_[i] > thread_epoch[thread_id][i])
            thread_epoch[thread_id][i] = tx->latest_[i];
    }
}

__attribute_noinline__ static void sth_ptm_abort() {
    Transaction *tx = TX(thread_tx_and_mode);
    tx->w_set_->FreeNew();
    tx->w_set_->ResetCurrTxStatus();
    tx->w_set_->Unlock();
    InitTransaction(tx);
    thread_abort_counter++;
    siglongjmp(*(tx->env_), 1);
}

void sth_ptm_clean_thread() {
    for(int i=0; i<kMaxThreadNum; i++) {
        thread_epoch[thread_id][i] = INF;
    }
}

#endif