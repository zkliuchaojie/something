#ifndef INTSET_LINKEDLIST_H_
#define INTSET_LINKEDLIST_H_

#ifndef STH_H_
#include "sth.h"
#endif

#ifndef MM_PARTITION_H_
#include "mm_partition.h"
#endif

#ifndef MM_POOL_H_
#include "mm_pool.h"
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
};

class LinkedList : public AbstractIntset {
public:
    // sentinel 哨兵
    PtmObjectWrapper<PONode> *sentinel_;

public:
    Pool<PONode> *po_node_pool_;

public:
    LinkedList() {
        PTM_START(RDWR);
        sentinel_ = new PtmObjectWrapper<PONode>();
        PONode *po_node = sentinel_->Open(INIT);
        po_node->next_ = sentinel_;
        PTM_COMMIT;
    };
    ~LinkedList() {
        delete po_node_pool_;
    }
    // if val exists, return true, or false
    bool Search(Value_t val);
    void Insert(Value_t val);
    // if val exists and is deleted successfully, return ture
    bool Delete(Value_t val);
    unsigned long long Size();
};

bool LinkedList::Search(Value_t val) {
    PTM_START(RDONLY);
    bool retval = false;
    PONode *node = sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper<PONode> *po_node_wrapper = node->next_;
    while(po_node_wrapper != sentinel_) {
        PONode *node = po_node_wrapper->Open(READ);
        if(node->val_ == val) {
            retval = true;
            break;
        }else {
            po_node_wrapper = node->next_;
        }
    }
    PTM_COMMIT;
    return retval;
}

void LinkedList::Insert(Value_t val) {
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
    PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
    PONode *new_po_node = new_po_node_wrapper->Open(INIT);
    new_po_node->val_ = val;
    new_po_node->next_ = curr;
    po_node->next_ = new_po_node_wrapper;
    PTM_COMMIT;
}

bool LinkedList::Delete(Value_t val) {
    PTM_START(RDWR);
    bool retval = true;
    // std::cout <<"delete: " << val << std::endl;
    PONode *tmp;
    PtmObjectWrapper<PONode> *prev = sentinel_;
    PONode *node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = node->next_;
    node = curr->Open(READ);
    while(curr != sentinel_ && val > node->val_) {
        // std::cout << "delete" << std::endl;
        prev = curr;
        curr = node->next_;
        node = curr->Open(READ);
    }
    if(val == node->val_) {
        // NOTE: we do not consider releasing the deleted node.
        curr->Open(WRITE);
        node->next_->Open(WRITE);
        tmp = prev->Open(WRITE);
        tmp->next_ = node->next_;
        retval = true;
    }else {
        retval = false;
    }
    PTM_COMMIT;
    return retval;
}

unsigned long long LinkedList::Size() {
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