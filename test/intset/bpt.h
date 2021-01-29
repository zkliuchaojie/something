#ifndef INTSET_BPT_H_
#define INTSET_BPT_H_

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

#include <queue>

#define ORDER 3

class PONode : public AbstractPtmObject {
public:
    void *pointers_[ORDER];
	Key_t keys_[ORDER-1];
	PtmObjectWrapper<PONode> *parent_;
	bool is_leaf_;
	int num_keys_;

public:
    PONode() : parent_(NULL), is_leaf_(false), num_keys_(0) {};
    ~PONode() {};
#ifndef PMDKTX_H_
    AbstractPtmObject *Clone() {
        PONode *po_node = new PONode();
        memcpy(po_node->pointers_, pointers_, sizeof(void *) * ORDER);
        memcpy(po_node->keys_, keys_, sizeof(Key_t)*(ORDER - 1));
        po_node->parent_ = parent_;
        po_node->is_leaf_ = is_leaf_;
        po_node->num_keys_ = num_keys_;
        return po_node;
    }
    void Copy(AbstractPtmObject *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        memcpy(pointers_, po_node->pointers_, sizeof(void *) * ORDER);
        memcpy(keys_, po_node->keys_, sizeof(Key_t)*(ORDER - 1));
        parent_ = po_node->parent_;
        is_leaf_ = po_node->is_leaf_ ;
        num_keys_ = po_node->num_keys_ ;
    }
#endif
};

class Bpt : public AbstractIntset {
public:
    // sentinel_ is pointing to root with pointers_[0];
    PtmObjectWrapper<PONode> *sentinel_;

public:
    Bpt() {
        PTM_START(RDWR);
#ifdef USE_AEP
        sentinel_ = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (sentinel_) PtmObjectWrapper<PONode>();
#else
        sentinel_ = new PtmObjectWrapper<PONode>();
#endif
        PONode *po_node = sentinel_->Open(WRITE);
        po_node->pointers_[0] = nullptr;
        PTM_COMMIT;
    }
    ~Bpt() {
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

private:
    void Transplant(PONode *old_parent,
                PtmObjectWrapper<PONode> *old_wrapper,
                PONode *old, 
                PtmObjectWrapper<PONode> *new_wrapper);
    PtmObjectWrapper<PONode> *find_leaf(Key_t key);
    void insert_into_leaf_after_splitting(PtmObjectWrapper<PONode> *leaf_wrapper, Key_t key, Value_t val);
    void insert_into_parent(PtmObjectWrapper<PONode> *left_wrapper, Key_t key, PtmObjectWrapper<PONode> *right_wrapper);
    void insert_into_node_after_splitting(PtmObjectWrapper<PONode> *old_node_wrapper, int left_index, \
                                          Key_t key, PtmObjectWrapper<PONode> *right_wrapper);
    void delete_entry(PtmObjectWrapper<PONode> *n_wrapper, Key_t key, void *pointer);
    void remove_entry_from_node(PtmObjectWrapper<PONode> *n_wrapper, Key_t key, void *pointer);
    void adjust_root();
    void coalesce_nodes(PtmObjectWrapper<PONode> *n_wrapper, PtmObjectWrapper<PONode> *neighbor_wrapper, \
                        int neighbor_index, Key_t k_prime);
    void redistribute_nodes(PtmObjectWrapper<PONode> *n_wrapper, PtmObjectWrapper<PONode> *neighbor_wrapper, \
                        int neighbor_index, int k_prime_index, Key_t k_prime);

    int cut(int length) {
	    if (length % 2 == 0)
		    return length/2;
	    else
		    return length/2 + 1;
    }

};

PtmObjectWrapper<PONode> *Bpt::find_leaf(Key_t key) {
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = (PtmObjectWrapper<PONode> *)sentinel_node->pointers_[0];
    PONode *curr;
    while (curr_wrapper != nullptr) {
        curr = curr_wrapper->Open(READ);
        if (curr->is_leaf_ == true)
            break;
        int i;
        for (i=0; i<curr->num_keys_; i++) {
            if (key < curr->keys_[i])
                break;
        }
        curr_wrapper = (PtmObjectWrapper<PONode> *)curr->pointers_[i];
    }
    return curr_wrapper;
}

Value_t Bpt::Get(Key_t key) {
#ifdef PMDKTX_H_
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
    PtmObjectWrapper<PONode> *leaf_wrapper = find_leaf(key);
    if (leaf_wrapper == nullptr) {
        PTM_COMMIT;
        return 0;
    } else {
        PONode *leaf = leaf_wrapper->Open(READ);
        for (int i=0; i<leaf->num_keys_; i++) {
            if (leaf->keys_[i] == key) {
                PTM_COMMIT;
                return 1;
            }
        }
        PTM_COMMIT;
        return 0;
    }
}

unsigned long long Bpt::Size() {
#ifdef PMDKTX_H_
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
    unsigned long long retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = (PtmObjectWrapper<PONode> *)sentinel_node->pointers_[0];
    if (curr_wrapper == nullptr) {
        PTM_COMMIT;
        return 0;
    } else {
        PONode *curr = curr_wrapper->Open(READ);
        while (curr->is_leaf_ == false) {
            curr_wrapper = (PtmObjectWrapper<PONode> *)curr->pointers_[0];
            curr = curr_wrapper->Open(READ);
        }
        while (curr_wrapper != nullptr) {
            curr = curr_wrapper->Open(READ);
            retval += curr->num_keys_;
            curr_wrapper = (PtmObjectWrapper<PONode> *)curr->pointers_[ORDER-1];
        }
        PTM_COMMIT;
        return retval;
    }
}

void Bpt::insert_into_leaf_after_splitting(PtmObjectWrapper<PONode> *leaf_wrapper, Key_t key, Value_t val) {
    Key_t temp_keys[ORDER];
    void *temp_pointers[ORDER];
    int insertion_index, split, i, j;
    Key_t new_key;
#ifdef USE_AEP
    PtmObjectWrapper<PONode> *new_leaf_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
    new (new_leaf_wrapper) PtmObjectWrapper<PONode>();
#else
    PtmObjectWrapper<PONode> *new_leaf_wrapper = new PtmObjectWrapper<PONode>();
#endif
    PONode *new_leaf = new_leaf_wrapper->Open(WRITE);
    new_leaf->is_leaf_ = true;
    PONode *leaf = leaf_wrapper->Open(WRITE);

    insertion_index = 0;
    while (insertion_index < ORDER - 1 && key > leaf->keys_[insertion_index]) {
        insertion_index++;
    }
    for (i = 0, j = 0; i < leaf->num_keys_; i++, j++) {
        if (j == insertion_index)
            j++;
        temp_keys[j] = leaf->keys_[i];
        temp_pointers[j] = leaf->pointers_[i];
    }
    temp_keys[insertion_index] = key;
    temp_pointers[insertion_index] = (void *)val;

    leaf->num_keys_ = 0;
    split = cut(ORDER - 1);

    for (i = 0; i < split; i++) {
        leaf->keys_[i] = temp_keys[i];
        leaf->pointers_[i] = temp_pointers[i];
        leaf->num_keys_++;
    }

    for ( i = split, j = 0; i < ORDER; i++, j++) {
        new_leaf->keys_[j] = temp_keys[i];
        new_leaf->pointers_[j] = temp_pointers[i];
        new_leaf->num_keys_++;
    }

    new_leaf->pointers_[ORDER - 1] = leaf->pointers_[ORDER - 1];
    leaf->pointers_[ORDER - 1] = new_leaf_wrapper;

    new_leaf->parent_ = leaf->parent_;
    new_key = new_leaf->keys_[0];

    return insert_into_parent(leaf_wrapper, new_key, new_leaf_wrapper);
}

void Bpt::insert_into_parent(PtmObjectWrapper<PONode> *left_wrapper, Key_t key, PtmObjectWrapper<PONode> *right_wrapper) {
    int left_index;
    PtmObjectWrapper<PONode> *parent_wrapper;

    PONode *left = left_wrapper->Open(WRITE);
    PONode *right = right_wrapper->Open(WRITE);

    parent_wrapper = left->parent_;
    if (parent_wrapper == nullptr) {
#ifdef USE_AEP
        PtmObjectWrapper<PONode> *new_po_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (new_po_node_wrapper) PtmObjectWrapper<PONode>();
#else
        PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
        PONode *new_po_node = new_po_node_wrapper->Open(WRITE);
        new_po_node->keys_[0] = key;
        new_po_node->pointers_[0] = left_wrapper;
        new_po_node->pointers_[1] = right_wrapper;
        new_po_node->num_keys_++;
        new_po_node->parent_ = NULL;
        left->parent_ = new_po_node_wrapper;
        right->parent_ = new_po_node_wrapper;
        sentinel_->Open(WRITE)->pointers_[0] = new_po_node_wrapper;
        return;
    }

    PONode *parent = parent_wrapper->Open(WRITE);
    left_index = 0;
    while (left_index <= parent->num_keys_ && \
           parent->pointers_[left_index] != left_wrapper)
        left_index++;
    if (parent->num_keys_ < ORDER - 1) {
        int i;
        for (i = parent->num_keys_; i > left_index; i--) {
            parent->pointers_[i+1] = parent->pointers_[i];
            parent->keys_[i] = parent->keys_[i - 1];
        }
        parent->pointers_[left_index + 1] = right_wrapper;
        parent->keys_[left_index] = key;
        parent->num_keys_++;
        return;
    }
    return insert_into_node_after_splitting(parent_wrapper, left_index, key, right_wrapper);
}

void Bpt::insert_into_node_after_splitting(PtmObjectWrapper<PONode> *old_node_wrapper, int left_index, \
                                          Key_t key, PtmObjectWrapper<PONode> *right_wrapper) {
    int i, j, split;
    Key_t k_prime;
    PtmObjectWrapper<PONode> *child_wrapper;
    Key_t temp_keys[ORDER];
    void *temp_pointers[ORDER + 1];

    PONode *old_node = old_node_wrapper->Open(WRITE);
    PONode *right = right_wrapper->Open(WRITE);

    for (i = 0, j = 0; i < old_node->num_keys_ + 1; i++, j++) {
        if (j == left_index + 1)
            j++;
        temp_pointers[j] = old_node->pointers_[i];
    }

    for (i = 0, j = 0; i < old_node->num_keys_; i++, j++) {
        if (j == left_index)
            j++;
        temp_keys[j] = old_node->keys_[i];
    }

    temp_pointers[left_index + 1] = right_wrapper;
    temp_keys[left_index] = key;

    split = cut(ORDER);
#ifdef USE_AEP
    PtmObjectWrapper<PONode> *new_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
    new (new_node_wrapper) PtmObjectWrapper<PONode>();
#else
    PtmObjectWrapper<PONode> *new_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
    PONode *new_node = new_node_wrapper->Open(WRITE);
    old_node->num_keys_ = 0;
    for (i = 0; i < split - 1; i++) {
        old_node->pointers_[i] = temp_pointers[i];
        old_node->keys_[i] = temp_keys[i];
        old_node->num_keys_++;
    }
    old_node->pointers_[i] = temp_pointers[i];
    k_prime = temp_keys[split - 1];
    for (++i, j=0; i < ORDER; i++, j++) {
        new_node->pointers_[j] = temp_pointers[i];
        new_node->keys_[j] = temp_keys[i];
        new_node->num_keys_++;
    }
    new_node->pointers_[j] = temp_pointers[i];
    new_node->parent_ = old_node->parent_;

    for (i = 0; i <= new_node->num_keys_; i++) {
        child_wrapper = (PtmObjectWrapper<PONode> *)new_node->pointers_[i];
        child_wrapper->Open(WRITE)->parent_ = new_node_wrapper;
    }

    return insert_into_parent(old_node_wrapper, k_prime, new_node_wrapper);
}

int Bpt::Insert(Key_t key, Value_t val) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = (PtmObjectWrapper<PONode> *)sentinel_node->pointers_[0];
    if (curr_wrapper == nullptr) {
#ifdef USE_AEP
        PtmObjectWrapper<PONode> *new_po_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (new_po_node_wrapper) PtmObjectWrapper<PONode>();
#else
        PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
        PONode *new_po_node = new_po_node_wrapper->Open(WRITE);
        new_po_node->is_leaf_ = true;
        new_po_node->keys_[0] = key;
        new_po_node->pointers_[0] = (void *)val;
        new_po_node->pointers_[ORDER - 1] = nullptr;
        new_po_node->parent_ = nullptr;
        new_po_node->num_keys_++;
        sentinel_->Open(WRITE)->pointers_[0] = new_po_node_wrapper;
        PTM_COMMIT;
        return 1;
    }

    // leaf_wrapper can not be nullptr
    PtmObjectWrapper<PONode> *leaf_wrapper = find_leaf(key);
    PONode *leaf = leaf_wrapper->Open(READ);
    if (leaf->num_keys_ < ORDER - 1) {
        PONode *leaf = leaf_wrapper->Open(WRITE);
        int i, insertion_point;

        insertion_point = 0;
        while (insertion_point < leaf->num_keys_ && key > leaf->keys_[insertion_point]) {
            insertion_point++;
        }
        /* move keys and pointers */
        for (i = leaf->num_keys_; i > insertion_point; i--) {
            leaf->keys_[i] = leaf->keys_[i - 1];
            leaf->pointers_[i] = leaf->pointers_[i - 1];
        }
        leaf->keys_[insertion_point] = key;
        leaf->pointers_[insertion_point] = (void *)val;
        leaf->num_keys_++;
        PTM_COMMIT;
        return 1;
    }

    /* leaf must be split */
    insert_into_leaf_after_splitting(leaf_wrapper, key, val);
    PTM_COMMIT;
    return 1;
}

void Bpt::delete_entry(PtmObjectWrapper<PONode> *n_wrapper, Key_t key, void *pointer) {
    int min_keys;
    int neighbor_index = 0;
    int k_prime_index;
    Key_t k_prime;
    int capacity;
    int i;
    PtmObjectWrapper<PONode> *neighbor_wrapper;
    PONode *neighbor;

    remove_entry_from_node(n_wrapper, key, pointer);
    if (n_wrapper == sentinel_->Open(READ)->pointers_[0])
        return adjust_root();

    PONode *n = n_wrapper->Open(READ);
    min_keys = n->is_leaf_ ? cut(ORDER - 1) : cut(ORDER) - 1;
    if (n->num_keys_ >= min_keys)
        return;
    /* get neighbor index */
    PONode *parent = n->parent_->Open(READ);
    for (i = 0; i <= parent->num_keys_; i++) {
        if (parent->pointers_[i] == n_wrapper) {
            neighbor_index = i - 1;
            break;
        }
    }
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = parent->keys_[k_prime_index];
    neighbor_wrapper = neighbor_index == -1 ? (PtmObjectWrapper<PONode> *)parent->pointers_[1] : \
                       (PtmObjectWrapper<PONode> *)parent->pointers_[neighbor_index];
    neighbor = neighbor_wrapper->Open(READ);
    capacity = n->is_leaf_ ? ORDER : ORDER - 1;
    if (neighbor->num_keys_ + n->num_keys_ < capacity) {
        return coalesce_nodes(n_wrapper, neighbor_wrapper, neighbor_index, k_prime);
    } else {
        return redistribute_nodes(n_wrapper, neighbor_wrapper, neighbor_index, k_prime_index, k_prime);
    }
}

void Bpt::coalesce_nodes(PtmObjectWrapper<PONode> *n_wrapper, PtmObjectWrapper<PONode> *neighbor_wrapper, \
                        int neighbor_index, Key_t k_prime) {
    int i, j, neighbor_insertion_index, n_end;
    PtmObjectWrapper<PONode> *tmp;

    if (neighbor_index == -1) {
		tmp = n_wrapper;
		n_wrapper = neighbor_wrapper;
		neighbor_wrapper = tmp;
	}

    PONode *n = n_wrapper->Open(WRITE);
    PONode *neighbor = neighbor_wrapper->Open(WRITE);

    neighbor_insertion_index = neighbor->num_keys_;
    if (!n->is_leaf_) {
		neighbor->keys_[neighbor_insertion_index] = k_prime;
		neighbor->num_keys_++;

		n_end = n->num_keys_;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys_[i] = n->keys_[j];
			neighbor->pointers_[i] = n->pointers_[j];
			neighbor->num_keys_++;
			n->num_keys_--;
		}

		neighbor->pointers_[i] = n->pointers_[j];

		for (i = 0; i < neighbor->num_keys_ + 1; i++) {
			PtmObjectWrapper<PONode> *tmp_wrapper = (PtmObjectWrapper<PONode> *)neighbor->pointers_[i];
            PONode *tmp = tmp_wrapper->Open(WRITE);
			tmp->parent_ = neighbor_wrapper;
		}
	} else {
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys_; i++, j++) {
			neighbor->keys_[i] = n->keys_[j];
			neighbor->pointers_[i] = n->pointers_[j];
			neighbor->num_keys_++;
		}
		neighbor->pointers_[ORDER - 1] = n->pointers_[ORDER - 1];
	}

	return delete_entry(n->parent_, k_prime, n_wrapper);
}

void Bpt::redistribute_nodes(PtmObjectWrapper<PONode> *n_wrapper, PtmObjectWrapper<PONode> *neighbor_wrapper, \
                        int neighbor_index, int k_prime_index, Key_t k_prime) {
    int i;
    PtmObjectWrapper<PONode> *tmp_wrapper;
    PONode *n = n_wrapper->Open(WRITE);
    PONode *neighbor = neighbor_wrapper->Open(WRITE);

    if (neighbor_index != -1) {
        if (!n->is_leaf_)
            n->pointers_[n->num_keys_ + 1] = n->pointers_[n->num_keys_];
        for (i = n->num_keys_; i > 0; i--) {
            n->keys_[i] = n->keys_[i - 1];
            n->pointers_[i] = n->pointers_[i - 1];
        }
        if (!n->is_leaf_) {
            n->pointers_[0] = neighbor->pointers_[neighbor->num_keys_];
            tmp_wrapper = (PtmObjectWrapper<PONode> *)n->pointers_[0];
            PONode *tmp = tmp_wrapper->Open(WRITE);
            tmp->parent_ = n_wrapper;
            neighbor->pointers_[neighbor->num_keys_] = NULL;
            n->keys_[0] = k_prime;
            n->parent_->Open(WRITE)->keys_[k_prime_index] = neighbor->keys_[neighbor->num_keys_ - 1];
        } else {
            n->pointers_[0] = neighbor->pointers_[neighbor->num_keys_ - 1];
            neighbor->pointers_[neighbor->num_keys_ - 1] = NULL;
            n->keys_[0] = neighbor->keys_[neighbor->num_keys_ - 1];
            n->parent_->Open(WRITE)->keys_[k_prime_index] = n->keys_[0];
        }
    } else {
        if (n->is_leaf_) {
			n->keys_[n->num_keys_] = neighbor->keys_[0];
			n->pointers_[n->num_keys_] = neighbor->pointers_[0];
			n->parent_->Open(WRITE)->keys_[k_prime_index] = neighbor->keys_[1];
		} else {
			n->keys_[n->num_keys_] = k_prime;
			n->pointers_[n->num_keys_ + 1] = neighbor->pointers_[0];
			tmp_wrapper = (PtmObjectWrapper<PONode> *)n->pointers_[n->num_keys_ + 1];
            PONode *tmp = tmp_wrapper->Open(WRITE);
			tmp->parent_ = n_wrapper;
			n->parent_->Open(WRITE)->keys_[k_prime_index] = neighbor->keys_[0];
		}
		for (i = 0; i < neighbor->num_keys_ - 1; i++) {
			neighbor->keys_[i] = neighbor->keys_[i + 1];
			neighbor->pointers_[i] = neighbor->pointers_[i + 1];
		}
		if (!n->is_leaf_)
			neighbor->pointers_[i] = neighbor->pointers_[i + 1];
    }
    n->num_keys_++;
    neighbor->num_keys_--;

    return;
}

void Bpt::adjust_root() {
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *root_wrapper = (PtmObjectWrapper<PONode> *)sentinel_node->pointers_[0];
    PONode *root = root_wrapper->Open(READ);

    if (root->num_keys_ > 0)
        return;

    if (!root->is_leaf_) {
        root = root_wrapper->Open(WRITE);
        root->parent_ = nullptr;
        sentinel_->Open(WRITE)->pointers_[0] = root->pointers_[0];
    } else {
        sentinel_->Open(WRITE)->pointers_[0] = nullptr;
    }
}

void Bpt::remove_entry_from_node(PtmObjectWrapper<PONode> *n_wrapper, Key_t key, void *pointer) {
    int i, num_pointers;

    PONode *n = n_wrapper->Open(WRITE);
    i = 0;
    while (key != n->keys_[i])
        i++;
    for (++i; i < n->num_keys_; i++)
        n->keys_[i - 1] = n->keys_[i];
    num_pointers = n->is_leaf_ ? n->num_keys_ : n->num_keys_ + 1;
    i = 0;
    while (n->pointers_[i] != pointer)
        i++;
    for (++i; i < num_pointers; i++)
        n->pointers_[i-1] = n->pointers_[i];

    n->num_keys_--;

    return;
}

int Bpt::Delete(Key_t key) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    PtmObjectWrapper<PONode> *key_leaf_wrapper = find_leaf(key);
    if (key_leaf_wrapper == nullptr) {
        PTM_COMMIT;
        return 0;
    } else {
        PONode *key_leaf = key_leaf_wrapper->Open(WRITE);
        for (int i = 0; i < key_leaf->num_keys_; i++) {
            if (key == key_leaf->keys_[i]) {
                delete_entry(key_leaf_wrapper, key_leaf->keys_[i], key_leaf->pointers_[i]);
                PTM_COMMIT;
                return 1;
            }
        }
        PTM_COMMIT;
        return 0;
    }
}

#endif