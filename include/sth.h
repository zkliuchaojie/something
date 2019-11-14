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
    INIT,
};
class AbstractPtmObjectWrapper;
template <typename T>
class PtmObjectWrapper;
class Transaction;
static Transaction *GetThreadTransaction();
static void sth_ptm_abort(Transaction *tx);


struct PtmObjectWrapperWithVersionNum {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    unsigned long long version_num_;
};

class Transaction {
public:
    sigjmp_buf env_;
    int status_;
    unsigned long long start_;
    unsigned long long end_;
    bool is_readonly_;
    std::vector<PtmObjectWrapperWithVersionNum> r_set_;
    std::vector<PtmObjectWrapperWithVersionNum> w_set_; 

    Transaction() {
        status_ = UNDETERMINED;
        is_readonly_ = true;
        r_set_.reserve(256);
        r_set_.reserve(256);
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
    virtual void Delete() = 0;
};

class AbstractPtmObjectWrapper {
public:
    virtual void DeleteNew() = 0;
    virtual bool Validate(unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp) = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};

};

/*
 * ptm object wrapper
 */
template <typename T>
class PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    struct ObjectWithVersionNum {
        void *object_;
        unsigned long long version_num_;    // version_num_ can be a timestamp.
    };

public:
    PtmObjectWrapper() {
        // I am not clear when generating a new PtmObject
        Transaction *tx;
        tx = GetThreadTransaction();
        if(tx->status_ !=  ACTIVE)
            pr_info_and_exit("there is no active transactions");
        tx_ = tx;
        new_ = nullptr;
        new (&old_) T();
        version_num_ = 0;
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

    void DeleteNew() {
        new_->Delete();
        new_ = nullptr;
    }
    bool Validate(unsigned long long version_num) {
        if (version_num_ != version_num)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long commit_timestamp) {
        version_num_ = commit_timestamp;
        old_.Copy(new_);
        new_->Delete();
        new_ = nullptr;
    }
    void Unlock() {
        mutex_.unlock();
    }

private:
    Transaction *tx_;
    unsigned long long version_num_;    // version_num_ can be a timestamp.
    T old_;
    AbstractPtmObject *new_;
    std::mutex mutex_;
    // for now, this is not used, we just implement one-version.
    std::vector<ObjectWithVersionNum> olders;

    AbstractPtmObject *OpenWithRead(Transaction *tx) {
        //std::cout << "OpenWithRead" << std::endl;
        // we do not need to check if this object already exists in r_set_.
        if(version_num_ < tx->end_) {
            tx->start_ = std::max(tx->start_, version_num_);
            //tx->end_ = std::min(tx->end_, ATOMIC_LOAD(&global_timestamp));
            // there exists an concurrent bug
            tx->r_set_.push_back(PtmObjectWrapperWithVersionNum{this, version_num_});
            return &old_;
        }else {
            sth_ptm_abort(tx);
        }
    }
    /*
     * OpenWithInit is used to init a new PtmObjectWrapper.
     */
    AbstractPtmObject *OpenWithInit() {
        return &old_;
    }

    AbstractPtmObject *OpenWithWrite(Transaction *tx) {
        //std::cout << "OpenWithWrite" << std::endl;
        if(tx->is_readonly_ == true)
            tx->is_readonly_ = false;
        // check if we already put this object into write set
        for(int i=0; i<tx->w_set_.size(); i++) {
            if(tx->w_set_[i].ptm_object_wrapper_ == this)
                return new_;
        }
        if(version_num_ < tx->end_) {
            tx->start_ = std::max(tx->start_, version_num_);
            tx->end_ = std::min(tx->end_, ATOMIC_LOAD(&global_timestamp));
            if(mutex_.try_lock() == true) {
                // there exists an concurrent bug
                tx->w_set_.push_back(PtmObjectWrapperWithVersionNum{this, version_num_});
                new_ = old_.Clone();
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
    tx->r_set_.clear();
    tx->w_set_.clear();
    return &tx->env_;
}

static void sth_ptm_validate(Transaction *tx) {
    PtmObjectWrapperWithVersionNum *pow_v;
    for(int i=0; i<tx->r_set_.size(); i++) {
        pow_v = &tx->r_set_[i]; 
        if(pow_v->ptm_object_wrapper_->Validate(pow_v->version_num_) == false)
            sth_ptm_abort(tx);
    }
}

static void sth_ptm_commit() {
    Transaction *tx;
    tx = GetThreadTransaction();
    if(tx->status_ != ACTIVE)
        pr_info_and_exit("there is no active transactions");
    sth_ptm_validate(tx);
    unsigned long long commit_timestamp;
    commit_timestamp = ATOMIC_FETCH_ADD(&global_timestamp, 1);
    // commit writes
    for(int i=0; i<tx->w_set_.size(); i++) {
        tx->w_set_[i].ptm_object_wrapper_->CommitWrite(commit_timestamp);
    }
    for(int i=0; i<tx->w_set_.size(); i++) {
        tx->w_set_[i].ptm_object_wrapper_->Unlock();
    }
    tx->status_ = COMMITTED;
}

static void sth_ptm_abort(Transaction *tx) {
    tx->status_ = ACTIVE;
    tx->start_ = ATOMIC_LOAD(&global_timestamp);
    tx->end_ = INF;
    tx->is_readonly_ = true;
    tx->r_set_.clear();
    for(int i=0; i<tx->w_set_.size(); i++)
        tx->w_set_[i].ptm_object_wrapper_->DeleteNew();
    tx->w_set_.clear();
    siglongjmp(tx->env_, 1);
}

static Transaction *GetThreadTransaction() {
    thread_local Transaction *tx = nullptr;
    if(tx == nullptr)
        tx = new Transaction();
    return tx;
}

#endif