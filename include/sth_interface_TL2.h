#ifndef STH_INTERFACE_TL2_H_
#define STH_INTERFACE_TL2_H_

#ifndef STH_INTERFACE_H_
#include "sth_interface.h"
#endif

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#include <string.h>
#include <setjmp.h>
#include <thread>

#define PTM_THREAD_CLEAN do { \
    ; \
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

class ReadSet;
class WriteSet;
static void sth_ptm_abort();

enum OpenMode {
    READ = 0,
    WRITE,
};

enum TransactionMode {
    RDONLY = 0x1,
    RDWR   = 0x2,
};

struct ReadSetEntry {
    PtmObjectWrapperInterface *ptm_object_wrapper_;
    unsigned long long ti_and_ts_;
};

struct WriteSetEntry {
    PtmObjectWrapperInterface *ptm_object_wrapper_;
    unsigned long long ti_and_ts_;
    PtmObjectInterface *write_object_;
};

class ReadSet {
public:
    ReadSetEntry *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    ReadSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (ReadSetEntry *)malloc(sizeof(ReadSetEntry) * capacity_);
        memset(set_, 0, sizeof(ReadSetEntry) * capacity_);
    }
    ~ReadSet() {
        Clear();
        if (set_ != nullptr)
            delete set_;
    }
    void Clear() {
        entries_num_ = 0;
    }
    void Push(PtmObjectWrapperInterface *ptm_object_wrapper, unsigned long long ti_and_ts) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ti_and_ts_ = ti_and_ts;

        entries_num_++;
        if(entries_num_ > capacity_)
            pr_info_and_exit("overflow")
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ti_and_ts_) == false)
                return false;
        }
        return true;
    }
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
        for(int i=0; i<entries_num_; i++) {
            if (set_[i].write_object_ != nullptr)
                delete set_[i].write_object_;
        }
        entries_num_ = 0;
    }
    void Push(PtmObjectWrapperInterface *ptm_object_wrapper, unsigned long long ti_and_ts, 
        PtmObjectInterface *write_object) {
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ti_and_ts_ = ti_and_ts;
        set_[entries_num_].write_object_ = write_object;
        entries_num_++;
        if(entries_num_ > capacity_)
            pr_info_and_exit("overflow");
    }
    PtmObjectInterface* GetWrtieObjectBy(PtmObjectWrapperInterface *ptm_object_wrapper) {
        for(int i=0; i<entries_num_; i++) {
            if(set_[i].ptm_object_wrapper_ == ptm_object_wrapper)
                return set_[i].write_object_;
        }
        return nullptr;
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ti_and_ts_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long ti_and_ts) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ti_and_ts, set_[i].write_object_);
        }
    }
    void Lock() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Lock() == false) {
                for(i=i-1; i>=0; i--) {
                    set_[i].ptm_object_wrapper_->Unlock();
                }
                sth_ptm_abort();
            }
        }
    }
    void Unlock() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Unlock(); 
        }
    }
};

class Transaction: public SthTxInterface  {
public:
    TransactionMode mode_;
    ReadSet         *r_set_;
    WriteSet        *w_set_;
    sigjmp_buf      *env_;

public:
    Transaction() {
        r_set_ = new ReadSet();
        w_set_ = new WriteSet();
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

thread_local Transaction thread_tx;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public PtmObjectWrapperInterface {
public:
    PtmObjectWrapper() {
        lock_ = 0;
    }

    T *Open(OpenMode mode) {
        if (mode == READ) {
            return OpenWithRead();
        } else if (mode == WRITE) {
            return OpenWithWrite();
        }
        return nullptr;
    }
    bool Validate(unsigned long long ti_and_ts) {
        if (object_.ti_and_ts_ != ti_and_ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ti_and_ts, PtmObjectInterface *write_object) {
        object_.Copy(write_object);
        object_.ti_and_ts_ = ti_and_ts;
    }
    bool Lock() {
        int tmp = 0;
        return CAS(&lock_, &tmp, 1);
    }
    void Unlock() {
        // lock_ = 0 is enough
        lock_ = 0;
    }
private:
    int lock_;
    T object_;

    // for now, we didn't consider release T*
    T* OpenWithRead() {
retry:
        unsigned long long curr_ti_and_ts = object_.ti_and_ts_;
        if (lock_ != 0)
            sth_ptm_abort();
        if (thread_tx.CmpClocks(curr_ti_and_ts) == true) {
            T *ret = (T *)object_.Clone();
            if (lock_ != 0 || object_.ti_and_ts_ != curr_ti_and_ts)
                goto retry;
            if (thread_tx.mode_ == RDWR) {
                thread_tx.r_set_->Push(this, curr_ti_and_ts);
            }
            return ret;
        } else {
            if (thread_tx.mode_ == RDWR) {
                if (thread_tx.r_set_->Validate() && thread_tx.w_set_->Validate()) {
                    thread_tx.UpdateEnds(curr_ti_and_ts);
                    goto retry;
                } else {
                    sth_ptm_abort();
                }
            } else {
                // for read only tx, we abort directly
                sth_ptm_abort(); 
            }
        }
    }
    T *OpenWithWrite() {
retry:
        unsigned long long curr_ti_and_ts = object_.ti_and_ts_;
        if (lock_ != 0)
            sth_ptm_abort();
        if (thread_tx.CmpClocks(curr_ti_and_ts) == true) {
            T *ret;
            ret = (T *)thread_tx.w_set_->GetWrtieObjectBy(this);
            if (ret != nullptr) {
                return ret;
            }
            ret = (T *)object_.Clone();
            if (lock_ != 0 || object_.ti_and_ts_ != curr_ti_and_ts)
                goto retry;
            thread_tx.w_set_->Push(this, curr_ti_and_ts, ret);
            return ret;
        } else {
            if (thread_tx.r_set_->Validate() && thread_tx.w_set_->Validate()) {
                thread_tx.UpdateEnds(curr_ti_and_ts);
                goto retry;
            } else {
                    sth_ptm_abort();
            }
        }
    }
};

static void InitTransaction() {
    thread_tx.r_set_->Clear();
    thread_tx.w_set_->Clear();
    for(int i=0; i<kMaxThreadNum; i++) {
        thread_tx.ends_[i] = thread_clocks_[i];
    }
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
    thread_tx.mode_ = mode;
    InitTransaction();
    return thread_tx.env_;
}

static void sth_ptm_commit() {
    if (thread_tx.mode_ == RDWR) {
        thread_tx.w_set_->Lock();
        if (thread_tx.r_set_->Validate() == false ||
            thread_tx.w_set_->Validate() == false) {
            thread_tx.w_set_->Unlock();
            sth_ptm_abort();
        }
        unsigned long long commit_ts = *thread_tx.thread_clock_ + 1;
        thread_tx.w_set_->CommitWrites(TI_AND_TS(thread_tx.thread_id_, commit_ts));
        thread_tx.w_set_->Unlock();
        *thread_tx.thread_clock_ = commit_ts;
    }
}

static void sth_ptm_abort() {
    thread_abort_counter++;
    InitTransaction();
    siglongjmp(*(thread_tx.env_), 1);
}

#endif