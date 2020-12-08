#ifndef INTSET_RBT_H_
#define INTSET_RBT_H_

#ifndef STH_H_
#include "sth.h"
#endif

#ifndef INTSET_H_
#include "intset.h"
#endif

#include <queue>

enum Color {
    BLACK = 0,
    RED = 1,
};

class PONode : public AbstractPtmObject {
public:
    Value_t val_;
    PtmObjectWrapper<PONode> *left_;
    PtmObjectWrapper<PONode> *right_;
    PtmObjectWrapper<PONode> *parent_;
    Color color_;

public:
    PONode() : val_(0), left_(nullptr), right_(nullptr),
               parent_(nullptr), color_(BLACK) {};
    ~PONode() {};
    AbstractPtmObject *Clone() {
        PONode *po_node = new PONode();
        po_node->val_ = val_;
        po_node->left_ = left_;
        po_node->right_ = right_;
        po_node->parent_ = parent_;
        po_node->color_ = color_;
        return po_node;
    }
    void Copy(AbstractPtmObject *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        val_ = po_node->val_;
        left_ = po_node->left_;
        right_ = po_node->right_;
        parent_ = po_node->parent_;
        color_ = po_node->color_;
    }
};

class Rbt : public AbstractIntset {
public:
    // sentinel_ is pointing to root_ with left_,
    // which is used to prorect "root"
    PtmObjectWrapper<PONode> *sentinel_;
    // nil_
    PtmObjectWrapper<PONode> *nil_;

public:
    Rbt() {
        PTM_START(RDWR);
        // init nil_
#ifdef USE_AEP
        nil_ = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (nil_) PtmObjectWrapper<PONode>();
#else
        nil_ = new PtmObjectWrapper<PONode>();
#endif
        PONode *nil_node = nil_->Open(WRITE);
        nil_node->left_ = nullptr;
        nil_node->right_ = nullptr;
        nil_node->parent_ = nullptr;
        nil_node->color_ = BLACK;

        // init sentinel_
#ifdef USE_AEP
        sentinel_ = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (sentinel_) PtmObjectWrapper<PONode>();
#else
        sentinel_ = new PtmObjectWrapper<PONode>();
#endif
        PONode *sentinel_node = sentinel_->Open(WRITE);
        sentinel_node->left_ = nil_;    // this is root
        sentinel_node->right_ = nullptr;
        sentinel_node->parent_ = nullptr;
        sentinel_node->color_ = BLACK;
        
        PTM_COMMIT;
    }
    ~Rbt() {
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
    void inorder_print(PtmObjectWrapper<PONode> *wrapper);
    void inorder_print();
    void preorder_print(PtmObjectWrapper<PONode> *wrapper);
    void preorder_print();

private:
    void Insert_Fixup(PtmObjectWrapper<PONode> *curr_wrapper);
    void Left_Rotate(PtmObjectWrapper<PONode> *wrapper);
    void Right_Rotate(PtmObjectWrapper<PONode> *wrapper);
    void Transplant(PONode *old_parent,
                PtmObjectWrapper<PONode> *old_wrapper,
                PONode *old,
                PtmObjectWrapper<PONode> *new_wrapper);
    void Delete_Fixup(PtmObjectWrapper<PONode> *curr_wrapper);
};

Value_t Rbt::Get(Key_t key) {
    PTM_START(RDONLY);
    Value_t retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_node->left_;
    PONode *curr;
    while (curr_wrapper != nil_) {
        curr = curr_wrapper->Open(READ);
        if (key == curr->val_) {
            retval = 1;
            break;
        } else if (key > curr->val_) {
            curr_wrapper = curr->right_;
        } else {
            curr_wrapper = curr->left_;
        }
    }
    PTM_COMMIT;
    return retval;
}

void Rbt::Left_Rotate(PtmObjectWrapper<PONode> *x_wrapper) {
    PONode *x = x_wrapper->Open(WRITE);
    PtmObjectWrapper<PONode> *y_wrapper = x->right_;
    PONode *y = y_wrapper->Open(WRITE);
    x->right_ = y->left_;
    if (y->left_ != nil_) {
        y->left_->Open(WRITE)->parent_ = x_wrapper;
    }
    y->parent_ = x->parent_;
    if (x->parent_ == nil_) {
        sentinel_->Open(WRITE)->left_ = y_wrapper;
    } else if (x_wrapper == x->parent_->Open(WRITE)->left_) {
        x->parent_->Open(WRITE)->left_ = y_wrapper;
    } else {
        x->parent_->Open(WRITE)->right_ = y_wrapper;
    }
    y->left_ = x_wrapper;
    x->parent_ = y_wrapper;
}

void Rbt::Right_Rotate(PtmObjectWrapper<PONode> *y_wrapper) {
    PONode *y = y_wrapper->Open(WRITE);
    PtmObjectWrapper<PONode> *x_wrapper = y->left_;
    PONode *x = x_wrapper->Open(WRITE);
    y->left_ = x->right_;
    if (x->right_ != nil_) {
        x->right_->Open(WRITE)->parent_ = y_wrapper;
    }
    x->parent_ = y->parent_;
    if (y->parent_ == nil_) {
        sentinel_->Open(WRITE)->left_ = x_wrapper;
    } else if (y_wrapper == y->parent_->Open(WRITE)->left_) {
        y->parent_->Open(WRITE)->left_ = x_wrapper;
    } else {
        y->parent_->Open(WRITE)->right_ = x_wrapper;
    }
    x->right_ = y_wrapper;
    y->parent_ = x_wrapper;
}

/*
 *
 */
void Rbt::Insert_Fixup(PtmObjectWrapper<PONode> *curr_wrapper) {
    PONode *curr = curr_wrapper->Open(WRITE);
    PtmObjectWrapper<PONode> *parent_wrapper = curr->parent_;
    PONode *parent = parent_wrapper->Open(WRITE);
    while (parent->color_ == RED) {
        PONode *parent_parent = parent->parent_->Open(WRITE);
        if (parent_wrapper == parent_parent->left_) {
            PONode *y = parent_parent->right_->Open(WRITE);
            if (y->color_ == RED) {
                parent->color_ = BLACK;
                y->color_ = BLACK;
                parent_parent->color_ = RED;
                curr_wrapper = parent->parent_;
                curr = curr_wrapper->Open(WRITE);
                parent_wrapper = curr->parent_;
                parent = parent_wrapper->Open(WRITE);
            } else {
                if (curr_wrapper == parent->right_) {
                    curr_wrapper = curr->parent_;
                    Left_Rotate(curr_wrapper);
                    curr = curr_wrapper->Open(WRITE);
                    parent_wrapper = curr->parent_;
                    parent = parent_wrapper->Open(WRITE);
                }
                parent->color_ = BLACK;
                parent_parent->color_ = RED;
                Right_Rotate(parent->parent_);
            }
        } else {
            PONode *y = parent_parent->left_->Open(WRITE);
            if (y->color_ == RED) {
                parent->color_ = BLACK;
                y->color_ = BLACK;
                parent_parent->color_ = RED;
                curr_wrapper = parent->parent_;
                curr = curr_wrapper->Open(WRITE);
                parent_wrapper = curr->parent_;
                parent = parent_wrapper->Open(WRITE);
            } else {
                if (curr_wrapper == parent->left_) {
                    curr_wrapper = curr->parent_;
                    Right_Rotate(curr_wrapper);
                    curr = curr_wrapper->Open(WRITE);
                    parent_wrapper = curr->parent_;
                    parent = parent_wrapper->Open(WRITE);
                }
                parent->color_ = BLACK;
                parent_parent->color_ = RED;
                Left_Rotate(parent->parent_);
            }
        }
    }
    sentinel_->Open(WRITE)->left_->Open(WRITE)->color_ = BLACK;
}

int Rbt::Insert(Key_t key, Value_t val) {
    PTM_START(RDWR);
    // std::cout <<"insert: " << key << std::endl;
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_;
    PtmObjectWrapper<PONode> *saved_curr_wrapper = curr_wrapper;
    PONode *curr;
    while (curr_wrapper != nil_) {
        curr = curr_wrapper->Open(READ);
        saved_curr_wrapper = curr_wrapper;
        if (saved_curr_wrapper == sentinel_) {
            curr_wrapper = curr->left_;
        } else {
            if (key > curr->val_) {
                curr_wrapper = curr->right_;
            } else {
                curr_wrapper = curr->left_;
            }
        }
    }
#ifdef USE_AEP
        PtmObjectWrapper<PONode> *new_po_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (new_po_node_wrapper) PtmObjectWrapper<PONode>();
#else
        PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
    PONode *new_po_node = new_po_node_wrapper->Open(WRITE);
    new_po_node->val_ = val;
    new_po_node->left_ = nil_;
    new_po_node->right_ = nil_;
    new_po_node->parent_ = (saved_curr_wrapper == sentinel_) ? nil_ : saved_curr_wrapper;
    new_po_node->color_ = RED;
    curr = saved_curr_wrapper->Open(WRITE);
    if (saved_curr_wrapper == sentinel_) {
        curr->left_ = new_po_node_wrapper;
    } else {
        if (key > curr->val_) {
            curr->right_ = new_po_node_wrapper;
        } else {
            curr->left_ = new_po_node_wrapper;
        }
    }

    Insert_Fixup(new_po_node_wrapper);

    PTM_COMMIT;
    return 1;
}

/*
 * old_parent and old should be opened with WRITE before calling this func.
 */
void Rbt::Transplant(PONode *old_parent,
                PtmObjectWrapper<PONode> *old_wrapper,
                PONode *old, 
                PtmObjectWrapper<PONode> *new_wrapper) {
    if (old->parent_ == nil_) {
        sentinel_->Open(WRITE)->left_ = new_wrapper;
    } else if (old_parent->left_ == old_wrapper) {
        old_parent->left_ = new_wrapper;
    } else {
        old_parent->right_ = new_wrapper;
    }
    new_wrapper->Open(WRITE)->parent_ = old->parent_;
}

void Rbt::Delete_Fixup(PtmObjectWrapper<PONode> *x_wrapper) {
    PONode *x = x_wrapper->Open(WRITE);
    PONode *x_parent = x->parent_->Open(WRITE);
    while(x_wrapper != sentinel_->Open(WRITE)->left_ && x->color_ == BLACK) {
        if (x_wrapper == x_parent->left_) {
            PtmObjectWrapper<PONode> *w_wrapper = x_parent->right_;
            PONode *w = w_wrapper->Open(WRITE);
            if (w->color_ == RED) {
                w->color_ = BLACK;
                x_parent->color_ = RED;
                Left_Rotate(x->parent_);
                w_wrapper = x_parent->right_;
                w = w_wrapper->Open(WRITE);
            }
            PONode *w_left = w->left_->Open(WRITE);
            PONode *w_right = w->right_->Open(WRITE);
            if (w_left->color_ == BLACK && w_right->color_ == BLACK) {
                w->color_ = RED;
                x_wrapper = x->parent_;
                x = x_wrapper->Open(WRITE);
                x_parent = x->parent_->Open(WRITE);
            } else {
                if (w_right->color_ == BLACK) {
                    w_left->color_ = BLACK;
                    w->color_ = RED;
                    Right_Rotate(w_wrapper);
                    w_wrapper = x_parent->right_;
                    w = w_wrapper->Open(WRITE);
                }
                w->color_ = x_parent->color_;
                x_parent->color_ = BLACK;
                w->right_->Open(WRITE)->color_ = BLACK;
                Left_Rotate(x->parent_);
                x_wrapper = sentinel_->Open(WRITE)->left_;
                x = x_wrapper->Open(WRITE);
                x_parent = x->parent_->Open(WRITE);
            }
        } else {
            PtmObjectWrapper<PONode> *w_wrapper = x_parent->left_;
            PONode *w = w_wrapper->Open(WRITE);
            if (w->color_ == RED) {
                w->color_ = BLACK;
                x_parent->color_ = RED;
                Right_Rotate(x->parent_);
                w_wrapper = x_parent->left_;
                w = w_wrapper->Open(WRITE);
            }
            PONode *w_left = w->left_->Open(WRITE);
            PONode *w_right = w->right_->Open(WRITE);
            if (w_right->color_ == BLACK && w_left->color_ == BLACK) {
                w->color_ = RED;
                x_wrapper = x->parent_;
                x = x_wrapper->Open(WRITE);
                x_parent = x->parent_->Open(WRITE);
            } else {
                if (w_left->color_ == BLACK) {
                    w_right->color_ = BLACK;
                    w->color_ = RED;
                    Left_Rotate(w_wrapper);
                    w_wrapper = x_parent->left_;
                    w = w_wrapper->Open(WRITE);
                }
                w->color_ = x_parent->color_;
                x_parent->color_ = BLACK;
                w->left_->Open(WRITE)->color_ = BLACK;
                Right_Rotate(x->parent_);
                x_wrapper = sentinel_->Open(WRITE)->left_;
                x = x_wrapper->Open(WRITE);
                x_parent = x->parent_->Open(WRITE);
            }
        }
    }
    x->color_ = BLACK;
}

int Rbt::Delete(Key_t key) {
    PTM_START(RDWR);
    bool retval = 0;
    // std::cout <<"delete: " << key << std::endl;
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_->Open(READ)->left_;
    PONode *curr;
    while (curr_wrapper != nil_) {
        curr = curr_wrapper->Open(READ);
        if (key == curr->val_) {
            break;
        } else if (key > curr->val_) {
            curr_wrapper = curr->right_;
        } else {
            curr_wrapper = curr->left_;
        }
    }
    if (curr_wrapper != nil_) {
        PtmObjectWrapper<PONode> *z_wrapper = curr_wrapper;
        PONode *z = z_wrapper->Open(WRITE);
        PONode *y = z;
        PtmObjectWrapper<PONode> *x_wrapper;
        Color y_original_color = y->color_;
        if (z->left_ == nil_) {
            x_wrapper = z->right_;
            Transplant(z->parent_->Open(WRITE), z_wrapper, z, z->right_);
        } else if (z->right_ == nil_) {
            x_wrapper = z->left_;
            Transplant(z->parent_->Open(WRITE), z_wrapper, z, z->left_);
        } else {
            // find the mininum from curr->right_
            PtmObjectWrapper<PONode> *y_wrapper = z->right_;
            y = y_wrapper->Open(READ);
            while (y->left_ != nil_) {
                y_wrapper = y->left_;
                y = y_wrapper->Open(READ);
            }
            y = y_wrapper->Open(WRITE);
            y_original_color = y->color_;
            x_wrapper = y->right_;
            if (y->parent_ == z_wrapper) {
                x_wrapper->Open(WRITE)->parent_ = y_wrapper;
            } else {
                Transplant(y->parent_->Open(WRITE), y_wrapper, y, y->right_);
                y->right_ = z->right_;
                y->right_->Open(WRITE)->parent_ = y_wrapper;
            }
            Transplant(z->parent_->Open(WRITE), z_wrapper, z, y_wrapper);
            y->left_ = z->left_;
            y->left_->Open(WRITE)->parent_ = y_wrapper;
            y->color_ = z->color_;
        }
        if (y_original_color == BLACK) {
            Delete_Fixup(x_wrapper);
        }
        retval = 1;
    }
    PTM_COMMIT;
    return retval;
}

unsigned long long Rbt::Size() {
    PTM_START(RDONLY);
    unsigned long long retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *wrapper = sentinel_node->left_;
    std::queue<PtmObjectWrapper<PONode> *> q;
    q.push(wrapper);
    while(!q.empty()) {
        wrapper = q.front();
        q.pop();
        if (wrapper != nil_) {
            retval++;
            PONode *node = wrapper->Open(READ);
            q.push(node->left_);
            q.push(node->right_);
        }
    }
    PTM_COMMIT;
    return retval;
}

void Rbt::inorder_print(PtmObjectWrapper<PONode> *wrapper) {
    if (wrapper != nil_) {
        inorder_print(wrapper->Open(READ)->left_);
        std::cout << wrapper->Open(READ)->val_ << ' ';
        inorder_print(wrapper->Open(READ)->right_);
    }
}

void Rbt::inorder_print() {
    PTM_START(RDONLY);
    inorder_print(sentinel_->Open(READ)->left_);
    PTM_COMMIT;
}

void Rbt::preorder_print(PtmObjectWrapper<PONode> *wrapper) {
    if (wrapper != nil_) {
        std::cout << wrapper->Open(READ)->val_ << ' ';
        preorder_print(wrapper->Open(READ)->left_);
        preorder_print(wrapper->Open(READ)->right_);
    }
}

void Rbt::preorder_print() {
    PTM_START(RDONLY);
    preorder_print(sentinel_->Open(READ)->left_);
    PTM_COMMIT;
}


#endif