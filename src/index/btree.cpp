#include "../../include/index/btree.h"

namespace flexql {

BTree::BTree() : root(nullptr) {}

BTree::~BTree() {
    clear();
}

void BTree::clear() {
    root = nullptr;
    node_pool.clear();
}

BTreeNode* BTree::allocNode(bool is_leaf) {
    node_pool.emplace_back(is_leaf);
    return &node_pool.back();
}

void BTree::destroyNode(BTreeNode* node) {
    (void)node; // Pool owns all memory
}

// ============ SEARCH ============

size_t BTree::search(int key, bool& found) const {
    found = false;
    if (!root) return 0;
    return searchNode(root, key, found);
}

size_t BTree::searchNode(const BTreeNode* node, int key, bool& found) const {
    auto it = std::lower_bound(node->keys, node->keys + node->num_keys, key);
    int i = std::distance(node->keys, it);

    if (i < node->num_keys && key == node->keys[i]) {
        if (node->is_leaf) {
            found = true;
            return node->vals[i];
        }
    }

    if (node->is_leaf) return 0; // Not found
    return searchNode(node->children[i], key, found);
}

std::vector<size_t> BTree::rangeSearch(int lo, int hi) const {
    std::vector<size_t> result;
    if (!root) return result;
    rangeNode(root, lo, hi, result);
    return result;
}

void BTree::rangeNode(const BTreeNode* node, int lo, int hi,
                      std::vector<size_t>& result) const {
    if (!node) return;

    int i = 0;
    // Find first index where keys[i] >= lo
    while (i < node->num_keys && node->keys[i] < lo) i++;

    if (node->is_leaf) {
        // Collect all keys in [lo, hi]
        while (i < node->num_keys && node->keys[i] <= hi) {
            result.push_back(node->vals[i]);
            i++;
        }
    } else {
        // Traverse children interleaved with keys
        for (; i <= node->num_keys; i++) {
            // Visit child[i] first (keys are between keys[i-1] and keys[i])
            if (i < node->num_keys) {
                rangeNode(node->children[i], lo, hi, result);
                if (node->keys[i] <= hi) {
                    // Internal node key is just a separator, don't add it (data is in leaves)
                }
                if (node->keys[i] > hi) break;
            } else {
                rangeNode(node->children[i], lo, hi, result);
            }
        }
    }
}

// ============ INSERT ============

void BTree::insert(int key, size_t row_id) {
    if (!root) {
        root = allocNode(true);
        root->keys[0] = key;
        root->vals[0] = row_id;
        root->num_keys = 1;
        return;
    }

    if (root->num_keys == MAX_KEYS) {
        // Root is full — split it
        BTreeNode* new_root = allocNode(false);
        new_root->children[0] = root;
        root = new_root;
        splitChild(root, 0);
    }
    insertNonFull(root, key, row_id);
}

void BTree::splitChild(BTreeNode* parent, int child_index) {
    BTreeNode* child = parent->children[child_index];
    BTreeNode* new_node = allocNode(child->is_leaf);
    new_node->num_keys = MIN_KEYS;

    // Copy upper half of child into new_node
    for (int j = 0; j < MIN_KEYS; j++) {
        new_node->keys[j] = child->keys[j + BTREE_ORDER];
        new_node->vals[j] = child->vals[j + BTREE_ORDER];
    }
    if (!child->is_leaf) {
        for (int j = 0; j <= MIN_KEYS; j++) {
            new_node->children[j] = child->children[j + BTREE_ORDER];
        }
    }

    // The median key moves up to parent
    int median_key = child->keys[MIN_KEYS];
    size_t median_val = child->vals[MIN_KEYS];
    child->num_keys = MIN_KEYS;

    // Shift parent's children right to make room
    for (int j = parent->num_keys; j >= child_index + 1; j--) {
        parent->children[j + 1] = parent->children[j];
    }
    parent->children[child_index + 1] = new_node;

    // Shift parent's keys right
    for (int j = parent->num_keys - 1; j >= child_index; j--) {
        parent->keys[j + 1] = parent->keys[j];
        parent->vals[j + 1] = parent->vals[j];
    }
    parent->keys[child_index] = median_key;
    parent->vals[child_index] = median_val;
    parent->num_keys++;
}

void BTree::insertNonFull(BTreeNode* node, int key, size_t row_id) {
    if (node->is_leaf) {
        auto it = std::upper_bound(node->keys, node->keys + node->num_keys, key);
        int i = std::distance(node->keys, it);

        // Shift keys right and insert
        for (int j = node->num_keys - 1; j >= i; j--) {
            node->keys[j + 1] = node->keys[j];
            node->vals[j + 1] = node->vals[j];
        }
        node->keys[i] = key;
        node->vals[i] = row_id;
        node->num_keys++;
    } else {
        // Find child to descend into
        auto it = std::upper_bound(node->keys, node->keys + node->num_keys, key);
        int i = std::distance(node->keys, it);

        if (node->children[i]->num_keys == MAX_KEYS) {
            splitChild(node, i);
            if (key > node->keys[i]) i++;
        }
        insertNonFull(node->children[i], key, row_id);
    }
}

// ============ DELETE ============

int BTree::findKey(const BTreeNode* node, int key) const {
    int idx = 0;
    while (idx < node->num_keys && node->keys[idx] < key) idx++;
    return idx;
}

void BTree::remove(int key) {
    if (!root) return;
    deleteNode(root, key);

    // If root became empty and has a child, make child the new root
    if (root->num_keys == 0 && !root->is_leaf) {
        // Pool still owns old root memory; just reassign pointer
        root = root->children[0];
    }
}

void BTree::deleteNode(BTreeNode* node, int key) {
    int idx = findKey(node, key);

    if (idx < node->num_keys && node->keys[idx] == key) {
        if (node->is_leaf) {
            removeFromLeaf(node, idx);
        } else {
            removeFromInternal(node, idx);
        }
    } else {
        if (node->is_leaf) return; // Key not found

        bool last_child = (idx == node->num_keys);
        if (node->children[idx]->num_keys < BTREE_ORDER) {
            fill(node, idx);
        }
        if (last_child && idx > node->num_keys) {
            deleteNode(node->children[idx - 1], key);
        } else {
            deleteNode(node->children[idx], key);
        }
    }
}

void BTree::removeFromLeaf(BTreeNode* node, int idx) {
    for (int i = idx + 1; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];
        node->vals[i - 1] = node->vals[i];
    }
    node->num_keys--;
}

void BTree::removeFromInternal(BTreeNode* node, int idx) {
    int key = node->keys[idx];

    if (node->children[idx]->num_keys >= BTREE_ORDER) {
        int pred = getPredecessor(node, idx);
        size_t pred_val = getPredecessorVal(node, idx);
        node->keys[idx] = pred;
        node->vals[idx] = pred_val;
        deleteNode(node->children[idx], pred);
    } else if (node->children[idx + 1]->num_keys >= BTREE_ORDER) {
        int succ = getSuccessor(node, idx);
        size_t succ_val = getSuccessorVal(node, idx);
        node->keys[idx] = succ;
        node->vals[idx] = succ_val;
        deleteNode(node->children[idx + 1], succ);
    } else {
        merge(node, idx);
        deleteNode(node->children[idx], key);
    }
}

int BTree::getPredecessor(BTreeNode* node, int idx) {
    BTreeNode* cur = node->children[idx];
    while (!cur->is_leaf) cur = cur->children[cur->num_keys];
    return cur->keys[cur->num_keys - 1];
}

size_t BTree::getPredecessorVal(BTreeNode* node, int idx) {
    BTreeNode* cur = node->children[idx];
    while (!cur->is_leaf) cur = cur->children[cur->num_keys];
    return cur->vals[cur->num_keys - 1];
}

int BTree::getSuccessor(BTreeNode* node, int idx) {
    BTreeNode* cur = node->children[idx + 1];
    while (!cur->is_leaf) cur = cur->children[0];
    return cur->keys[0];
}

size_t BTree::getSuccessorVal(BTreeNode* node, int idx) {
    BTreeNode* cur = node->children[idx + 1];
    while (!cur->is_leaf) cur = cur->children[0];
    return cur->vals[0];
}

void BTree::fill(BTreeNode* node, int idx) {
    if (idx != 0 && node->children[idx - 1]->num_keys >= BTREE_ORDER) {
        borrowFromPrev(node, idx);
    } else if (idx != node->num_keys && node->children[idx + 1]->num_keys >= BTREE_ORDER) {
        borrowFromNext(node, idx);
    } else {
        if (idx != node->num_keys) {
            merge(node, idx);
        } else {
            merge(node, idx - 1);
        }
    }
}

void BTree::borrowFromPrev(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx - 1];

    // Shift child's keys right by one
    for (int i = child->num_keys - 1; i >= 0; i--) {
        child->keys[i + 1] = child->keys[i];
        child->vals[i + 1] = child->vals[i];
    }
    if (!child->is_leaf) {
        for (int i = child->num_keys; i >= 0; i--) {
            child->children[i + 1] = child->children[i];
        }
    }

    child->keys[0] = node->keys[idx - 1];
    child->vals[0] = node->vals[idx - 1];
    if (!child->is_leaf) {
        child->children[0] = sibling->children[sibling->num_keys];
    }

    node->keys[idx - 1] = sibling->keys[sibling->num_keys - 1];
    node->vals[idx - 1] = sibling->vals[sibling->num_keys - 1];
    child->num_keys++;
    sibling->num_keys--;
}

void BTree::borrowFromNext(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx + 1];

    child->keys[child->num_keys] = node->keys[idx];
    child->vals[child->num_keys] = node->vals[idx];
    if (!child->is_leaf) {
        child->children[child->num_keys + 1] = sibling->children[0];
    }

    node->keys[idx] = sibling->keys[0];
    node->vals[idx] = sibling->vals[0];

    for (int i = 1; i < sibling->num_keys; i++) {
        sibling->keys[i - 1] = sibling->keys[i];
        sibling->vals[i - 1] = sibling->vals[i];
    }
    if (!sibling->is_leaf) {
        for (int i = 1; i <= sibling->num_keys; i++) {
            sibling->children[i - 1] = sibling->children[i];
        }
    }

    child->num_keys++;
    sibling->num_keys--;
}

void BTree::merge(BTreeNode* node, int idx) {
    BTreeNode* child = node->children[idx];
    BTreeNode* sibling = node->children[idx + 1];

    // Pull the separator key from parent into child at position MIN_KEYS
    child->keys[MIN_KEYS] = node->keys[idx];
    child->vals[MIN_KEYS] = node->vals[idx];

    // Copy sibling's keys/vals/children into child
    for (int i = 0; i < sibling->num_keys; i++) {
        child->keys[i + MIN_KEYS + 1] = sibling->keys[i];
        child->vals[i + MIN_KEYS + 1] = sibling->vals[i];
    }
    if (!child->is_leaf) {
        for (int i = 0; i <= sibling->num_keys; i++) {
            child->children[i + MIN_KEYS + 1] = sibling->children[i];
        }
    }
    child->num_keys += sibling->num_keys + 1;

    // Remove separator from parent
    for (int i = idx + 1; i < node->num_keys; i++) {
        node->keys[i - 1] = node->keys[i];
        node->vals[i - 1] = node->vals[i];
    }
    for (int i = idx + 2; i <= node->num_keys; i++) {
        node->children[i - 1] = node->children[i];
    }
    node->num_keys--;

    // Pool owns sibling memory; no delete needed
}

} // namespace flexql
