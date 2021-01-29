#ifndef INTSET_LINKEDLIST_H_
#define INTSET_LINKEDLIST_H_

// #ifndef STH_STH_H_
// #include "sth.h"
// #endif

// #ifndef DUDETM_H_
// #include "dudetm.h"
// #endif

#ifndef PMDKTX_H_
#include "pmdktx.h"
#endif

#ifndef INTSET_H_
#include "intset.h"
#endif

/*
 * PO means Ptm Object.
 */
class PONode : public AbstractPtmObject {
public:
    Value_t val_;
    PtmObjectWrapper<PONode> *next_;

public:
    PONode() : val_(0), next_(nullptr) {};
    PONode(Value_t val) : val_(val), next_(nullptr) {};
    PONode(Value_t val, PtmObjectWrapper<PONode> *next) : val_(val), next_(next) {};
    ~PONode() {};
#ifndef PMDKTX_H_
    AbstractPtmObject *Clone() {
        PONode *po_node = new PONode();
        po_node->val_ = val_;
        po_node->next_ = next_;
        return po_node;
    }
    void Copy(AbstractPtmObject *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        val_ = po_node->val_;
        next_ = po_node->next_;
    }
#endif
};

class LinkedList : public AbstractIntset {
public:
    // sentinel 哨兵
    PtmObjectWrapper<PONode> *sentinel_;

public:
    LinkedList() {
        PTM_START(RDWR);
#ifdef USE_AEP
        sentinel_ = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (sentinel_) PtmObjectWrapper<PONode>();
#else
        sentinel_ = new PtmObjectWrapper<PONode>();
#endif
        PONode *po_node = sentinel_->Open(WRITE);
        po_node->next_ = sentinel_;
        PTM_COMMIT;
    };
    ~LinkedList() {
        ;
    }
    // if exists, return 1, else return 0
    Value_t Get(Key_t key);
    // do not support update operation
    bool Update(Key_t key, Value_t val) {return false;}
    // key is equal to val
    int Insert(Key_t key, Value_t val);
    int Delete(Key_t key);
    unsigned long long Size();
#ifdef PMDKTX_H_
private:
    std::shared_mutex rw_lock_;
#endif
};

Value_t LinkedList::Get(Key_t key) {
#ifdef PMDKTX_H_
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
     Value_t retval=0;
    PONode *node = sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper<PONode> *po_node_wrapper = node->next_;
    while(po_node_wrapper != sentinel_) {
        PONode *node = po_node_wrapper->Open(READ);
        if(node->val_ == key) {
            retval = 1;
            break;
        }else {
            po_node_wrapper = node->next_;
        }
    }
    PTM_COMMIT;
    return retval;
}

int LinkedList::Insert(Key_t key, Value_t val) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    // std::cout <<"insert: " << val << std::endl;
    PtmObjectWrapper<PONode> *prev = sentinel_;
    PONode *node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = node->next_;

    node = curr->Open(READ);

    while(curr != sentinel_ && val > node->val_) {
        // std::cout << "insert" << std::endl;
        prev = curr;
        curr = node->next_;
        node = curr->Open(READ);
    }
    PONode *po_node = nullptr;
    curr->Open(WRITE);
    po_node = prev->Open(WRITE);
#ifdef USE_AEP
        PtmObjectWrapper<PONode> *new_po_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (new_po_node_wrapper) PtmObjectWrapper<PONode>();
#else
        PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
    PONode *new_po_node = new_po_node_wrapper->Open(WRITE);
    new_po_node->val_ = val;
    new_po_node->next_ = curr;
    po_node->next_ = new_po_node_wrapper;
    PTM_COMMIT;
    return 1;
}

int LinkedList::Delete(Key_t key) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    bool ret = 0;
    // std::cout <<"delete: " << key << std::endl;
    PONode *tmp;
    PtmObjectWrapper<PONode> *prev = sentinel_;
    PONode *node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = node->next_;
    node = curr->Open(READ);
    while(curr != sentinel_ && key > node->val_) {
        // std::cout << "delete" << std::endl;
        prev = curr;
        curr = node->next_;
        node = curr->Open(READ);
    }
    if(key == node->val_) {
        // NOTE: we do not consider releasing the deleted node.
        curr->Open(WRITE);
        node->next_->Open(WRITE);
        tmp = prev->Open(WRITE);
        tmp->next_ = node->next_;
        ret = 1;
    }else {
        ret = 0;
    }
    PTM_COMMIT;
    return ret;
}

unsigned long long LinkedList::Size() {
#ifdef PMDKTX_H_
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
    unsigned long long retval = 0;
    PONode *node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = node->next_;
    while(curr != sentinel_) {
        retval++;
        node = curr->Open(READ);
        curr = node->next_;
    }
    PTM_COMMIT;
    return retval;
}

#endif