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

#define USE_MM

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
    AbstractPtmObject *Clone(bool is_use_mm = true) {
        PONode *po_node;
        if (is_use_mm == true) {
#ifdef USE_MM
        po_node = ((Pool<PONode> *)__mm_pool_)->Alloc();
#else
        po_node = (PONode *)malloc(sizeof(PONode));
        new (po_node) PONode();
#endif
        } else {
            po_node = (PONode *)malloc(sizeof(PONode));
            new (po_node) PONode();
        }
        po_node->val_ = val_;
        po_node->next_ = next_;
        return po_node;
    }
    void Copy(AbstractPtmObject *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        val_ = po_node->val_;
        next_ = po_node->next_;
    }
    void Free() {
#ifdef USE_MM
        ((Pool<PONode> *)__mm_pool_)->Free(this);
#else
        ;
#endif
    }
    static void Destroy(void *object) {
        MMAbstractObject *object_to_free = (MMAbstractObject *)object;
        object_to_free->__next_=object_to_free->__owner_partition_->free_list_;
        object_to_free->__owner_partition_->free_list_ = object_to_free;
        object_to_free->__owner_partition_->allocated_num_--;
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
        po_node_pool_ = new Pool<PONode>();
        PTM_START;
        sentinel_ = new PtmObjectWrapper<PONode>();
        PONode *po_node = (PONode *)sentinel_->Open(INIT);
        po_node->next_ = sentinel_;
        po_node->__mm_pool_ = po_node_pool_;
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
    PTM_START;
    bool retval = false;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper<PONode> *po_node_wrapper = po_node->next_;
    while(po_node_wrapper != sentinel_) {
        po_node = (PONode *)po_node_wrapper->Open(READ);
        if(po_node->val_ == val) {
            retval = true;
            break;
        }else {
            po_node_wrapper = po_node->next_;
        }
    }
    PTM_COMMIT;
    return retval;
}

void LinkedList::Insert(Value_t val) {
    PTM_START;
    //std::cout <<"insert: " << val << std::endl;
    PtmObjectWrapper<PONode> *prev = sentinel_;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = po_node->next_;

    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val > po_node->val_) {
        prev = curr;
        curr = po_node->next_;
        po_node = (PONode *)curr->Open(READ);
    }
    curr->Open(WRITE);
    po_node = (PONode *)prev->Open(WRITE);
    PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
    PONode *new_po_node = (PONode *)new_po_node_wrapper->Open(INIT);
    new_po_node->val_ = val;
    new_po_node->next_ = curr;
    new_po_node->__mm_pool_ = po_node_pool_;
    po_node->next_ = new_po_node_wrapper;
    PTM_COMMIT;
}

bool LinkedList::Delete(Value_t val) {
    PTM_START;
    PONode *tmp;
    bool retval = true;
    PtmObjectWrapper<PONode> *prev = sentinel_;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr = po_node->next_;

    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val > po_node->val_) {
        prev = curr;
        curr = po_node->next_;
        po_node = (PONode *)curr->Open(READ);
    }
    if(val == po_node->val_) {
        // NOTE: we do not consider releasing the deleted node.
        curr->Open(WRITE);
        po_node->next_->Open(WRITE);
        tmp = (PONode *)prev->Open(WRITE);
        tmp->next_ = po_node->next_;
        retval = true;
    }else {
        retval = false;
    }
    PTM_COMMIT;
    return retval;
}

unsigned long long LinkedList::Size() {
    PTM_START;
    unsigned long long retval = 0;
    // skip the sentinel
    PtmObjectWrapper<PONode> *po_node_wrapper = ((PONode *)sentinel_->Open(READ))->next_;
    while(po_node_wrapper != sentinel_) {
        retval++;
        po_node_wrapper = ((PONode *)po_node_wrapper->Open(READ))->next_;
    }
    PTM_COMMIT;
    return retval;
}

#endif