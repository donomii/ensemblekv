#include <stddef.h>

#include "binary_tree_plus.h"

#define BTREE_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "Assertion failed: %s\n", message); \
            abort(); \
        } \
    } while (0)

// Helper functions for offset<->pointer conversions
static inline btree *btree_from_offset(mega_pool *p, ii_int offset) {
    if (offset == 0) return NULL;
    return (btree *)((char *)p->recommended_address + offset);
}

static inline ii_int btree_to_offset(mega_pool *p, btree *ptr) {
    if (ptr == NULL) return 0;
    return (ii_int)((char *)ptr - (char *)p->recommended_address);
}
#include "binary_tree_plus.h"

#define BTREE_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "Assertion failed: %s\n", message); \
            abort(); \
        } \
    } while (0)


    int btree_memcmp(const void *a, const void *b, int lena, int lenb)
    {
        // Keep all original comments
        //printf("mymemcmp: same length, comparing '%s'(%d) and '%s'(%d)\n", a , lena, b, lenb);
        
        // Add assertions for validation instead of silent failure
        BTREE_ASSERT(a != NULL || lena == 0, "NULL pointer 'a' with non-zero length");
        BTREE_ASSERT(b != NULL || lenb == 0, "NULL pointer 'b' with non-zero length");
    
        if (lena < lenb)
        {
            return -1;
        }
        if (lenb < lena)
        {
            return 1;
        }
    
        int res = memcmp(a, b, lena);
        //printf("memcmp returned: %d\n", res);
        return res;
    }

ii_int btree_insert(mega_pool *p, ii_int tree_offset, void *key, ii_int key_length, void *data, ii_int data_length)
{
    btree *tree = btree_from_offset(p, tree_offset);
    btree *temp = NULL;
    if (!tree) {
        temp = (btree *)mega_malloc(p, sizeof(btree));
        if (!temp) {
            fprintf(stderr, "btree_insert: Memory allocation failed\n");
            return 0;
        }
        temp->bumper = 0;
        temp->left = 0;
        temp->right = 0;
        temp->key = key;
        temp->key_length = key_length;
        temp->data = data;
        temp->data_length = data_length;
        temp->bumperl = 0;
        printf("pool %p, size %d, start free %p\n", p, p->size, p->start_free);
        printf("Allocated new node at %p\n", temp);
        return btree_to_offset(p, temp);
    }

    if (!key || key_length <= 0) {
        fprintf(stderr, "btree_insert: Invalid key parameters\n");
        return tree_offset;
    }
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        fprintf(stderr, "btree_insert: Data corruption detected!\n");
        return tree_offset;
    }
    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);
    if (comp < 0) {
        ii_int left_offset = btree_insert(p, tree->left, key, key_length, data, data_length);
        tree->left = left_offset;
    }
    else if (comp > 0) {
        ii_int right_offset = btree_insert(p, tree->right, key, key_length, data, data_length);
        tree->right = right_offset;
    }
    // If comp == 0, we found a match but don't update it (that's what btree_set is for)
    return btree_to_offset(p, tree);
}

int btree_depth(mega_pool *p, ii_int tree_offset, int depth)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (tree) {
        if ((tree->bumper != 0) || (tree->bumperl != 0)){
            printf("tree> Data corruption!\n");
            abort();
        }
        int l = btree_depth(p, tree->left, depth + 1);
        int r = btree_depth(p, tree->right, depth + 1);
        return (l > r) ? l : r;
    } else {
        return depth;
    }
}


// Count the number of nodes in the tree
int btree_count(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (tree) {
        return 1 + btree_count(p, tree->left) + btree_count(p, tree->right);
    }
    return 0;
}


union All_u {
    void * v;
   uint64_t ui;
   int64_t i;
   float f;
   char str[16];
} all_u;

typedef union All_u all;

typedef struct {
    all car;
    all cdr;
} allpair;


allpair btree_balance_rec(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    allpair ret;
    ret.car.ui = 0;
    ret.cdr.v = NULL;
#ifdef DOUBLECHECK
    int startcount = btree_count(p, tree_offset);
#endif
    if (!tree)
        return ret;
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        printf("tree> Data corruption!\n");
        exit(1);
        abort();
    }
    allpair p_left = btree_balance_rec(p, tree->left);
    uint64_t l_depth = p_left.car.ui;
    tree->left = (p_left.cdr.v ? btree_to_offset(p, (btree *)p_left.cdr.v) : 0);
    allpair p_right = btree_balance_rec(p, tree->right);
    tree->right = (p_right.cdr.v ? btree_to_offset(p, (btree *)p_right.cdr.v) : 0);
    uint64_t r_depth = p_right.car.ui;
    // Keep all ASCII diagrams and comments in rotation code
    btree *new_right = btree_from_offset(p, tree->right);
    if ((r_depth - l_depth > 2) && (new_right != NULL) && (btree_from_offset(p, new_right->left) != NULL)) {
        // Rotate left
        btree *pnode = tree;
        check_in_pool(p, btree_from_offset(p, pnode->right), "balance_rec: right");
        btree *q = btree_from_offset(p, pnode->right);
        /*     p
             X   q 
               Y   Z */
        check_in_pool(p, btree_from_offset(p, q->left), "balance_rec: left");
        pnode->right = q->left;
        /*     
              p    q 
            X   Y   Z */
        q->left = btree_to_offset(p, pnode);
        /*      q
              p   Z   
            X   Y    */
        tree = q;
    }
    btree *new_left = btree_from_offset(p, tree->left);
    if ((l_depth - r_depth > 2) && (new_left != NULL) && (btree_from_offset(p, new_left->right) != NULL)) {
        btree *q = tree;
        check_in_pool(p, btree_from_offset(p, q->left), "balance_rec: left");
        btree *pnode = btree_from_offset(p, q->left);
        /*      q
              p   Z   
            X   Y    */
        check_in_pool(p, btree_from_offset(p, pnode->right), "balance_rec: right");
        q->left = pnode->right;
        /*      
              p   q   
            X   Y   Z */
        pnode->right = btree_to_offset(p, q);
        /*      p
              X   q   
                Y   Z */
        tree = pnode;
    }
#ifdef DOUBLECHECK
    int endcount = btree_count(p, btree_to_offset(p, tree));
    if (startcount != endcount) {
        printf("Balancing dropped some nodes!\n");
        abort();
    }
#endif
    ret.car.ui = (l_depth + r_depth + 1);
    ret.cdr.v = tree;
    return ret;
}


ii_int btree_balance(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    d("Balancing %p\n", tree);
#ifdef DOUBLECHECK
    if (!btree_validate(p, tree)) {
        printf("Tree validation failed before balancing\n");
        abort();
    }
#endif
    allpair pr = btree_balance_rec(p, tree_offset);
#ifdef DOUBLECHECK
    btree* result = (btree*)pr.cdr.v;
    if (!btree_validate(p, result)) {
        printf("Tree validation failed after balancing\n");
        abort();
    }
#endif
    return btree_to_offset(p, (btree*)pr.cdr.v);
}

ii_int btree_set(mega_pool *p, ii_int tree_offset, void const *key, ii_int key_length, void *data, ii_int data_length)
{
    btree *tree = btree_from_offset(p, tree_offset);
    check_in_pool(p, key, "set: key");
    check_in_pool(p, data, "set: data");
    btree *temp = NULL;
    if (!tree) {
        temp = (btree *)mega_malloc(p, sizeof(btree));
        temp->bumper = 0;
        temp->left = 0;
        temp->right = 0;
        temp->key = key;
        temp->key_length = key_length;
        temp->data = data;
        temp->data_length = data_length;
        temp->bumperl = 0;
        return btree_to_offset(p, temp);
    }
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        printf("tree> Data corruption!\n");
        abort();
    }
    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);
    if (comp < 0) {
        ii_int left_offset = btree_set(p, tree->left, key, key_length, data, data_length);
        tree->left = left_offset;
    }
    else if (comp > 0) {
        ii_int right_offset = btree_set(p, tree->right, key, key_length, data, data_length);
        tree->right = right_offset;
    }
    else if (comp == 0) {
        tree->data = data;
        tree->data_length = data_length;
    }
    return btree_to_offset(p, tree);
}

void btree_print_preorder(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (tree) {
        printf("%s\n", (char*)tree->key);
        btree_print_preorder(p, tree->left);
        btree_print_preorder(p, tree->right);
    }
}
void btree_print_inorder(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (tree) {
        btree_print_inorder(p, tree->left);
        printf("%s\n", (char*)tree->key);
        btree_print_inorder(p, tree->right);
    }
}
void btree_dump_string_hash(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if ((void *)tree > (void *)10) {
        btree_dump_string_hash(p, tree->left);
        printf("\n\n****%s:\n", (char*)tree->key);
        ll_dump_string_list(tree->data);
        btree_dump_string_hash(p, tree->right);
    }
}
void btree_print_postorder(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (tree) {
        btree_print_postorder(p, tree->left);
        btree_print_postorder(p, tree->right);
        printf("%s\n", (char*)tree->key);
    }
}

// Helper function to delete a node from btree (by offset)
ii_int btree_delete_node(mega_pool *p, ii_int root_offset, ii_int node_to_delete_offset)
{
    btree *root = btree_from_offset(p, root_offset);
    btree *node_to_delete = btree_from_offset(p, node_to_delete_offset);
    if (!root) return 0;
    btree *left = btree_from_offset(p, root->left);
    btree *right = btree_from_offset(p, root->right);
    btree *temp = root;
    if (root == node_to_delete) {
        // Case 1: No children
        if (!left && !right) {
            return 0;
        }
        // Case 2: One child
        else if (!left) {
            temp = right;
            return btree_to_offset(p, temp);
        }
        else if (!right) {
            temp = left;
            return btree_to_offset(p, temp);
        }
        // Case 3: Two children
        else {
            // Find inorder successor (smallest node in right subtree)
            btree *successor = right;
            while (successor && btree_from_offset(p, successor->left)) {
                successor = btree_from_offset(p, successor->left);
            }
            // Copy successor's data to this node
            root->key = successor->key;
            root->key_length = successor->key_length;
            root->data = successor->data;
            root->data_length = successor->data_length;
            // Delete the successor
            ii_int right_offset = btree_delete_node(p, root->right, btree_to_offset(p, successor));
            root->right = right_offset;
        }
    }
    else if (btree_memcmp(node_to_delete->key, root->key,
                          node_to_delete->key_length, root->key_length) < 0) {
        ii_int left_offset = btree_delete_node(p, root->left, node_to_delete_offset);
        root->left = left_offset;
    }
    else {
        ii_int right_offset = btree_delete_node(p, root->right, node_to_delete_offset);
        root->right = right_offset;
    }
    return btree_to_offset(p, root);
}
ii_int btree_search(mega_pool *p, ii_int tree_offset, const void *key, ii_int key_length)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (!tree)
        return 0;
    BTREE_ASSERT(key != NULL || key_length == 0, "NULL key with non-zero length in btree_search");
    check_in_pool(p, tree, "search: tree");
    check_in_pool(p, btree_from_offset(p, tree->left), "search: left");
    check_in_pool(p, btree_from_offset(p, tree->right), "search: right");
    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);
    if (comp < 0) {
        return btree_search(p, tree->left, key, key_length);
    }
    else if (comp > 0) {
        return btree_search(p, tree->right, key, key_length);
    }
    else if (comp == 0) {
        return btree_to_offset(p, tree);
    }
    printf("tree search> Could not find %s, returning NULL\n", (const char*)key);
    printf("Impossible!\n");
    abort();
}

int btree_check_integrity(mega_pool *p, ii_int tree_offset, int *issues_found)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (!tree) return 1;  // Empty tree is valid
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        if (issues_found) (*issues_found)++;
        fprintf(stderr, "Tree integrity check: Corruption markers detected\n");
        return 0;
    }
    if (!tree->key && tree->key_length > 0) {
        if (issues_found) (*issues_found)++;
        fprintf(stderr, "Tree integrity check: NULL key with non-zero length\n");
        return 0;
    }
    int left_ok = btree_check_integrity(p, tree->left, issues_found);
    int right_ok = btree_check_integrity(p, tree->right, issues_found);
    return left_ok && right_ok;
}



int btree_validate(mega_pool *p, ii_int tree_offset)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (!tree) return 1; // Empty tree is valid
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        printf("Tree corruption detected: non-zero bumper values\n");
        return 0;
    }
    if (tree->key == NULL && tree->key_length > 0) {
        printf("Tree corruption detected: NULL key with length %d\n", tree->key_length);
        return 0;
    }
    int left_valid = 1;
    int right_valid = 1;
    btree *left = btree_from_offset(p, tree->left);
    btree *right = btree_from_offset(p, tree->right);
    if (left) {
        left_valid = btree_validate(p, tree->left);
        if (left_valid && btree_memcmp(left->key, tree->key, left->key_length, tree->key_length) >= 0) {
            printf("BST property violation: left child key >= parent key\n");
            return 0;
        }
    }
    if (right) {
        right_valid = btree_validate(p, tree->right);
        if (right_valid && btree_memcmp(right->key, tree->key, right->key_length, tree->key_length) <= 0) {
            printf("BST property violation: right child key <= parent key\n");
            return 0;
        }
    }
    return left_valid && right_valid;
}


void btree_test_tree(mega_pool *p)
{
    ii_int root_offset = 0;
    ii_int tmp_offset;
    /* Inserting nodes into tree */
    root_offset = btree_insert(p, root_offset, mega_insert(p, "Inserting", 10), 10, "a", 2);
    root_offset = btree_insert(p, root_offset, mega_insert(p, "nodes", 6), 6, "a", 2);
    root_offset = btree_insert(p, root_offset, mega_insert(p, "into", 5), 5, "a", 2);
    root_offset = btree_insert(p, root_offset, mega_insert(p, "tree", 5), 5, "a", 2);
    /* Printing nodes of tree */
    printf("Pre Order Display\n");
    btree_print_preorder(p, root_offset);
    printf("In Order Display\n");
    btree_print_inorder(p, root_offset);
    printf("Post Order Display\n");
    btree_print_postorder(p, root_offset);
    /* Search node into tree */
    tmp_offset = btree_search(p, root_offset, mega_insert(p, "tree", 5), 5);
    btree *tmp = btree_from_offset(p, tmp_offset);
    if (tmp)
    {
        printf("Searched node=%d\n", (int)(intptr_t)tmp->data);
    }
    else
    {
        printf("Data Not found in tree->\n");
    }
    /* Deleting all nodes of tree */
    //deltree(root);
}


int btree_iterate(mega_pool *p, ii_int tree_offset, int (*callback)(const void*, int, void*, int, void*), void* userdata)
{
    btree *tree = btree_from_offset(p, tree_offset);
    if (!btree_validate(p, tree_offset)) {
        printf("tree corrupted\n");
        abort();
    }
    printf("Iterating %p\n", tree);
    if (tree) {
        printf("Left %p, right %p, key %p, data %p\n", btree_from_offset(p, tree->left), btree_from_offset(p, tree->right), tree->key, tree->data);
        int quit = btree_iterate(p, tree->left, callback, userdata);
        if (quit) return 1;
        quit = callback(tree->key, tree->key_length, tree->data, tree->data_length, userdata);
        if (quit) return 1;
        quit = btree_iterate(p, tree->right, callback, userdata);
        return 0;
    }
    return 1;
}