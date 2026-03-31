#ifndef FLEXQL_BTREE_H
#define FLEXQL_BTREE_H

#include <vector>
#include <deque>
#include <cstddef>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace flexql {

// In-memory B-Tree for primary key indexing.
// Order 64: each node holds up to 127 keys, min 63 keys (except root).
// Leaf nodes store (int_key -> row_id) pairs.
// row_id is the physical position in the ColumnStore columns[] arrays.

static constexpr int BTREE_ORDER = 64; // max children per node = 2*ORDER
static constexpr int MAX_KEYS    = 2 * BTREE_ORDER - 1;
static constexpr int MIN_KEYS    = BTREE_ORDER - 1;

struct BTreeNode {
    int      keys[MAX_KEYS];
    size_t   vals[MAX_KEYS]; // row_ids (valid only in leaves)
    BTreeNode* children[MAX_KEYS + 1];
    int      num_keys;
    bool     is_leaf;

    BTreeNode(bool leaf = true)
        : num_keys(0), is_leaf(leaf)
    {
        memset(children, 0, sizeof(children));
    }
};

class BTree {
public:
    BTree();
    ~BTree();

    // Insert key -> row_id mapping
    void insert(int key, size_t row_id);

    // Exact search. Returns row_id if found, sets found=true. O(log n).
    size_t search(int key, bool& found) const;

    // Collect all row_ids for keys in [lo, hi] inclusive. O(log n + k).
    std::vector<size_t> rangeSearch(int lo, int hi) const;

    // Remove the entry for key. O(log n).
    void remove(int key);

    // Clear entire tree
    void clear();

    bool empty() const { return root == nullptr || root->num_keys == 0; }

private:
    BTreeNode* root;

    // Node pool allocator — contiguous memory for cache-friendly access
    std::deque<BTreeNode> node_pool;
    BTreeNode* allocNode(bool is_leaf);

    void destroyNode(BTreeNode* node);
    void splitChild(BTreeNode* parent, int child_index);
    void insertNonFull(BTreeNode* node, int key, size_t row_id);

    // Search helpers
    size_t searchNode(const BTreeNode* node, int key, bool& found) const;
    void rangeNode(const BTreeNode* node, int lo, int hi,
                   std::vector<size_t>& result) const;

    // Delete helpers
    void deleteNode(BTreeNode* node, int key);
    int  findKey(const BTreeNode* node, int key) const;
    void removeFromLeaf(BTreeNode* node, int idx);
    void removeFromInternal(BTreeNode* node, int idx);
    int  getPredecessor(BTreeNode* node, int idx);
    size_t getPredecessorVal(BTreeNode* node, int idx);
    int  getSuccessor(BTreeNode* node, int idx);
    size_t getSuccessorVal(BTreeNode* node, int idx);
    void fill(BTreeNode* node, int idx);
    void borrowFromPrev(BTreeNode* node, int idx);
    void borrowFromNext(BTreeNode* node, int idx);
    void merge(BTreeNode* node, int idx);
};

} // namespace flexql

#endif // FLEXQL_BTREE_H
