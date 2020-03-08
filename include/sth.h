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

// #define PTM_START(tx_mode) do { \
//     sigjmp_buf *_e = sth_ptm_start(tx_mode); \
//     if (_e != NULL) { \
//         /* save the current signal mask */ \
//         sigsetjmp(*_e, 1); \
//     } \
// }while(0);

#define PTM_START(tx_mode)      \
    sth_ptm_start(tx_mode);     \
__sth_ptm_start:                \
    try {                       \

// #define PTM_COMMIT do { \
//     sth_ptm_commit(); \
// }while(0);

#define PTM_COMMIT              \
    sth_ptm_commit();           \
    } catch(TransactionExceptionAbort e) {  \
        goto __sth_ptm_start;   \
    }                           \

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
volatile unsigned long long thread_timestamps[kMaxThreadNum]={0};

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
    virtual void Free() = 0;
};

class AbstractPtmObjectWrapper {
public: 
    virtual void FreeNew() = 0;
    virtual bool Validate(int pos, unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp, volatile TransactionStatus *tx_status) = 0;
    virtual void IncPos() = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct PtmObjectAndSharedMutex {
    AbstractPtmObject *object_;
    std::shared_mutex *shared_mutex_;
};

template<typename T>
class UniqueReader {
public:
    UniqueReader(PtmObjectAndSharedMutex obj_and_mutex) {
        object_ = (T *)obj_and_mutex.object_;
        shared_mutex_ = obj_and_mutex.shared_mutex_;
    }
    ~UniqueReader() {
        // std::cout << "1: unlock shared, " << thread_id <<  std::endl;
        // printf("%p\n", shared_mutex_);
        shared_mutex_->unlock_shared();
    }
    // UniqueReader& operator=(UniqueReader reader) {
    //     if(object_ != NULL) {
    //         std::cout << "2: unlock shared" << std::endl;
    //         shared_mutex_->unlock_shared();
    //     }
    //     object_ = reader.object_;
    //     shared_mutex_ = reader.shared_mutex_;
    // }
    UniqueReader& operator=(PtmObjectAndSharedMutex obj_and_mutex) {
        if(object_ != NULL) {
            // std::cout << "2: unlock shared, " << thread_id << std::endl;
            // printf("%p\n", shared_mutex_);
            shared_mutex_->unlock_shared();
        }
        object_ = (T *)obj_and_mutex.object_;
        shared_mutex_ = obj_and_mutex.shared_mutex_;
    }
    const T *GetObject() {
        return object_;
    }

private:
    T *object_;
    std::shared_mutex *shared_mutex_;
    // delete copy function
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
    void CommitWrites(unsigned long long ti_and_ts, volatile TransactionStatus *tx_status) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ti_and_ts, tx_status);
        }
    }
    void IncPos() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->IncPos(); 
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

class Transaction {
public:
    /*
     * ends_ are used to record all threads' current time, and all
     * objects' commit time should be less or equal it.
     */
    unsigned long long ends_[kMaxThreadNum];
    RwSet *r_set_;
    RwSet *w_set_;
    // sigjmp_buf *env_;
    volatile TransactionStatus status_;

public:
    Transaction() {
        status_ = UNDETERMINED;
        r_set_ = new RwSet();
        w_set_ = new RwSet();
        // env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
    }
 
    ~Transaction() {
        if (r_set_ != nullptr)
            delete r_set_;
        if (w_set_ != nullptr)
            delete w_set_;
        // if (env_ != nullptr)
        //    free(env_);
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
        curr_tx_status_ = nullptr;     // init with nullptr
        for(int i=0; i<kVersionSize; i++)
            objs_[i].ti_and_ts_ = kInvalidTiAndTs;
        pos_ = 0;
        new_ = nullptr;
    }

    AbstractPtmObject *Open(OpenMode mode) {
        //std::cout << "Open" << std::endl;
        if (mode == WRITE) {
            //std::cout << "before OpenWithWrite" << std::endl;
            return OpenWithWrite();
        }
        else if (mode == INIT)
            return OpenWithInit();
    }

    /*
     * when OpenWithRead, it may be a Update Tx, but we don't know
     * until OpenWithWrite. So we will use old objects, and there is
     * a bug. For example, for a linkedlist, when deleting an object,
     * you search it first. In this process, if you use old objects,
     * you may can't find it, then the delete operation failed. So we
     * need users tell us whether it is a update transaction.
     */
    PtmObjectAndSharedMutex OpenWithRead() {
        // std::cout << "OpenWithRead, " << thread_id << std::endl;
        Transaction *tx = TX(thread_tx_and_mode);
        // pos_ maybe modifed by other tx, so we first get it, and if it is changed,
        // in OpenWithRead, which won't cause unconsitency. 
        int curr_pos = (kVersionSize+pos_-1)%kVersionSize;
        unsigned long long curr_ti_and_ts = objs_[curr_pos].ti_and_ts_; 
        // if ((curr_tx_status_ == nullptr || *curr_tx_status_ == COMMITTED) \
        //     && (TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)])) {
        if ((TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)])) {
            // if return false, which means no candidates can be used.
            if (objs_[curr_pos].shared_mutex_.try_lock_shared() == false) {
                // std::cout << "try lock shared, " << thread_id << std::endl;
                sth_ptm_abort();
            }
            // printf("%p\n", &objs_[curr_pos].shared_mutex_);
            if(curr_pos != ((kVersionSize+pos_-1)%kVersionSize) \
                || objs_[curr_pos].ti_and_ts_ != curr_ti_and_ts) {
                objs_[curr_pos].shared_mutex_.unlock_shared();
                // std::cout << "ti and ts changed, " << thread_id << std::endl;
                sth_ptm_abort();
            }
            if(MODE(thread_tx_and_mode) == RDWR)
                tx->r_set_->Push(this, curr_pos, curr_ti_and_ts);
            return PtmObjectAndSharedMutex{ &(objs_[curr_pos].object_), \
                                            &(objs_[curr_pos].shared_mutex_)};
        } else if (MODE(thread_tx_and_mode) == RDONLY) {
            for(int i=1; i<kVersionSize; i++) {
                curr_pos = (kVersionSize+curr_pos-1)%kVersionSize;
                curr_ti_and_ts = objs_[curr_pos].ti_and_ts_;
                if(curr_ti_and_ts == kInvalidTiAndTs)
                    break;
                if(TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)]) {
                    // if return false, which means no candidates can be used.
                    if (objs_[curr_pos].shared_mutex_.try_lock_shared() == false)
                        sth_ptm_abort();
                    if(objs_[curr_pos].ti_and_ts_ != curr_ti_and_ts) {
                        objs_[curr_pos].shared_mutex_.unlock_shared();
                        sth_ptm_abort();
                    }
                    return PtmObjectAndSharedMutex{ &(objs_[curr_pos].object_), \
                                                    &(objs_[curr_pos].shared_mutex_)};
                }
            }
        }
        if (MODE(thread_tx_and_mode) == RDONLY)
            thread_read_abort_counter++;
        // std::cout << "not find suitable objects, " << thread_id << std::endl; 
        // printf("%p\n", curr_tx_status_);
        // if(curr_tx_status_ != nullptr)
        //    std::cout << *curr_tx_status_ << std::endl;
        // std::cout << TS(curr_ti_and_ts) << std::endl;
        // std::cout << tx->ends_[TI(curr_ti_and_ts)] << std::endl;

        sth_ptm_abort();
    }

    void FreeNew() {
        new_->Free();
        new_ = nullptr;
    }
    bool Validate(int pos, unsigned long long ti_and_ts) {
        if (pos != (kVersionSize+pos_-1)%kVersionSize \
            || objs_[pos].ti_and_ts_ != ti_and_ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ti_and_ts, volatile TransactionStatus *tx_status) {
        if(objs_[pos_].shared_mutex_.try_lock()==false) {
            // std::cout << "shared_mutex_.try_lock() failed, " << thread_id << std::endl;
            // printf("%p\n", &objs_[pos_].shared_mutex_);
            sth_ptm_abort();
        }
        curr_tx_status_ = tx_status;
        /*
        * Since the copy func is not atomic, readers may read partial new data.
        * Therefore, readers must check the POW's transaction status.
        */
        objs_[pos_].ti_and_ts_ = ti_and_ts;
        mfence();
        objs_[pos_].object_.Copy(new_);
        objs_[pos_].shared_mutex_.unlock();
    }
    void IncPos() {
        pos_ = (pos_+1)%kVersionSize;
    }
    void Unlock() {
        mutex_.unlock();
    }
private:
    volatile TransactionStatus *curr_tx_status_;
    // pos_ is a index, that points the empty space that is used to store the latest version.
    volatile int pos_;
    struct OldObject {
        volatile unsigned long long ti_and_ts_;
        T object_;
        std::shared_mutex shared_mutex_;
    } objs_[kVersionSize];
    AbstractPtmObject *new_;
    std::mutex mutex_;

    /*
     * OpenWithInit is used to init a new PtmObjectWrapper.
     * Be sure that when initializing, no one can access it.
     */
    AbstractPtmObject *OpenWithInit() {
        objs_[pos_].ti_and_ts_ = 0;
        return &(objs_[pos_++].object_);
    }
    /*
     * The return value won't be seen by other threads, so we
     * do not need to use Pretect/Unprotect in mm_pool.
     */
    AbstractPtmObject *OpenWithWrite() {
        // std::cout << "OpenWithWrite, " << thread_id << std::endl;
        // if ((curr_tx_status_!=nullptr && *curr_tx_status_ != COMMITTED)) {
        //     sth_ptm_abort();
        // }
        Transaction *tx = TX(thread_tx_and_mode);
        int curr_pos = (kVersionSize+pos_-1)%kVersionSize;
        unsigned long long curr_ti_and_ts = objs_[curr_pos].ti_and_ts_; 
        // if ((curr_tx_status_ == nullptr || *curr_tx_status_ == COMMITTED) \
        //     && (TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)])) {
        if ((TS(curr_ti_and_ts) <= tx->ends_[TI(curr_ti_and_ts)])) {
            // check if we already put this object into write set
            if (tx->w_set_->DoesContain(this) == true)
                return new_;
            // std::cout << "before try lock" << std::endl;
            if(mutex_.try_lock() == true) {
                if(curr_pos != ((kVersionSize+pos_-1)%kVersionSize) \
                    || curr_ti_and_ts != objs_[curr_pos].ti_and_ts_) {
                    mutex_.unlock();
                    // std::cout << "object changed, " << thread_id << std::endl;
                    sth_ptm_abort();
                }
                tx->w_set_->Push(this, curr_pos, curr_ti_and_ts);
                if (new_ == nullptr)
                    new_ = objs_[curr_pos].object_.Clone();
                return new_;
            }
        }
        // std::cout << "not find suitable writable object, " << thread_id << std::endl;
        sth_ptm_abort();
    }
};

void InitTransaction(Transaction *tx) {
    for (int i=0; i<kMaxThreadNum; i++)
        tx->ends_[i] = thread_timestamps[i];
    tx->status_ = ACTIVE;
    tx->r_set_->Clear();
    tx->w_set_->Clear();
}

static void sth_ptm_validate(Transaction *tx) {
    // std::cout << tx->does_use_olders_ << std::endl;
    if (tx->r_set_->Validate() == false) {
        sth_ptm_abort();
    }
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
    if (thread_id == -1) {
        thread_id = ATOMIC_FETCH_ADD(&thread_id_allocator, 1);
        // std::cout << "thread_id: " << thread_id << std::endl;
    }
    Transaction *tx;
    if (thread_tx_and_mode == nullptr)
        tx = new Transaction();
    else
        tx = TX(thread_tx_and_mode);
    thread_tx_and_mode = TX_AND_MODE(tx, mode);
    InitTransaction(tx);
    //std::cout << "start ptm" << std::endl;
    // return tx->env_;
    return nullptr;
}

/*
 * do not consider durability yet.
 */
static void sth_ptm_commit() {
    Transaction *tx = TX(thread_tx_and_mode);
    if (MODE(thread_tx_and_mode) == RDWR) {
        sth_ptm_validate(tx);
        unsigned long long commit_ts = thread_timestamps[thread_id]+1;
        // commit writes
        volatile TransactionStatus *tx_status = new TransactionStatus;
        *tx_status = tx->status_;
        tx->w_set_->CommitWrites(TI_AND_TS(thread_id, commit_ts), tx_status);
        tx->w_set_->IncPos();
        tx->w_set_->Unlock();
        *tx_status = tx->status_ = COMMITTED;
        mfence();
        thread_timestamps[thread_id] = commit_ts;
    }
}

__attribute_noinline__ static void sth_ptm_abort() {
    Transaction *tx = TX(thread_tx_and_mode);
    tx->w_set_->FreeNew();
    tx->w_set_->Unlock();
    InitTransaction(tx);
    thread_abort_counter++;
    // std::cout << "abort, " << thread_id << std::endl;
    // siglongjmp(*(tx->env_), 1);
    throw TransactionExceptionAbort{};
}

#endif