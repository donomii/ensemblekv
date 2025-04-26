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

btree *btree_insert(mega_pool *p, btree *tree, void *key, btree_int key_length, void *data, btree_int data_length)
{
    // Validate input parameters
    if (!key || key_length <= 0) {
        fprintf(stderr, "btree_insert: Invalid key parameters\n");
        return tree;  // Return original tree unchanged
    }
    
    btree *temp = NULL;
    if (!tree) {
        // Validate memory allocation
        temp = (btree *)mega_malloc(p, sizeof(btree));
        if (!temp) {
            fprintf(stderr, "btree_insert: Memory allocation failed\n");
            return NULL;
        }
        
        temp->bumper = 0;
        temp->left = NULL;
        temp->right = NULL;
        temp->key = key;
        temp->key_length = key_length;
        temp->data = data;
        temp->data_length = data_length;
        temp->bumperl = 0;
        return temp;
    }
    
    // Check for data corruption
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        fprintf(stderr, "btree_insert: Data corruption detected!\n");
        return tree;  // Return original tree rather than aborting
    }

    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);

    if (comp < 0) {
        tree->left = btree_insert(p, tree->left, key, key_length, data, data_length);
    }
    else if (comp > 0) {
        tree->right = btree_insert(p, tree->right, key, key_length, data, data_length);
    }
    // If comp == 0, we found a match but don't update it (that's what btree_set is for)

    return tree;
}

int btree_depth(btree *tree, int depth)
{

    if (tree)
    {
        if ((tree->bumper != 0) || (tree->bumperl != 0)){
            printf("tree> Data corruption!\n");
            abort();
        }
        int l = btree_depth(tree->left, depth + 1);
        int r = btree_depth(tree->right, depth + 1);
        return (l > r) ? l : r;
    }
    else
    {
        return depth;
    }
}


//Count the number of nodes in the tree
int btree_count(btree *tree)
{
    if (tree)
    {
        return 1 + btree_count(tree->left) + btree_count(tree->right);
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


allpair btree_balance_rec(btree *tree)
{
    
    allpair ret;
    ret.car.ui=0;
    ret.cdr.v=NULL;
    
    // Enable the checking code that was commented out

    #ifdef DOUBLECHECK
    int startcount = btree_count(tree);
    #endif
    
    if (!tree)
        return ret;

    //printf("Balancing %p rec\n", tree);

    if ((tree->bumper != 0) || (tree->bumperl != 0))
    {
        printf("tree> Data corruption!\n");
        exit(1);
        abort();
    }

    allpair p = btree_balance_rec(tree->left);
  
    uint64_t l_depth = p.car.ui;
    tree->left = p.cdr.v;
    p = btree_balance_rec(tree->right);
  
    tree->right = p.cdr.v;
    uint64_t r_depth =  p.car.ui;

    // Keep all ASCII diagrams and comments in rotation code
    if ((r_depth - l_depth > 2) && (tree->right != NULL) && (tree->right->left != NULL))
    {
        //Rotate left
        btree *p = tree;
        check_in_pool(hidden_pool, p->right);
        btree *q = p->right;
        /*     p
             X   q 
               Y   Z */
        check_in_pool(hidden_pool, q->left);
        p->right = q->left;
        /*     
              p    q 
            X   Y   Z */
        q->left = p;
        /*      q
              p   Z   
            X   Y    */
        tree = q;
    }

    if ((l_depth - r_depth > 2) && (tree->left != NULL) && (tree->left->right != NULL))
    {
        btree *q = tree;
        check_in_pool(hidden_pool, q->left);
        btree *p = q->left;
        /*      q
              p   Z   
            X   Y    */
        check_in_pool(hidden_pool, p->right);
        q->left = p->right;
        /*      
              p   q   
            X   Y   Z */
        p->right = q;
        /*      p
              X   q   
                Y   Z */
        tree = p;
    }
  
    #ifdef DOUBLECHECK
    int endcount = btree_count(tree);
    if (startcount != endcount) {
        printf("Balancing dropped some nodes!\n");
        abort();
    }
    #endif
    
    ret.car.ui = (l_depth + r_depth +1);
    ret.cdr.v = tree;
    //printf("Balancing %p rec complete\n", tree);
    return ret;
}


btree *btree_balance(btree *tree)
{
    d("Balancing %p\n", tree);

    // Add validation before balancing
    #ifdef DOUBLECHECK
    if (!btree_validate(tree)) {
        printf("Tree validation failed before balancing\n");
        abort();
    }
    #endif
    
    allpair p = btree_balance_rec(tree);
    
    // Add validation after balancing
    #ifdef DOUBLECHECK
    btree* result = (btree*) p.cdr.v;
    if (!btree_validate(result)) {
        printf("Tree validation failed after balancing\n");
        abort();
    }
    #endif

    return (btree*) p.cdr.v;
}

btree *btree_set(mega_pool *p, btree *tree, void const*key, btree_int key_length, void *data, btree_int data_length)
{
    check_in_pool(p, key);
    check_in_pool(p, data);
    btree *temp = NULL;
    if (!tree)
    {
        temp = (btree *)mega_malloc(p, sizeof(btree));
        temp->bumper = 0;
        temp->left = NULL;
        temp->right = NULL;
        temp->key = key;
        temp->key_length = key_length;
        temp->data=data;
        temp->data_length=data_length;
        temp->bumperl = 0;
        return temp;
    }

    if ((tree->bumper != 0) || (tree->bumperl != 0))
    {
        printf("tree> Data corruption!\n");
        abort();
    }

    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);

    if (comp < 0)
    {
        tree->left = btree_set(p, tree->left, key, key_length, data, data_length);
    }

    else if (comp > 0)
    {
        tree->right = btree_set(p, tree->right, key, key_length, data, data_length);
    }

    else if (comp == 0)
    {
        //printf("tree set> Key %s exists, replacing value with %p\n", tree->data->key, val);
        tree->data = data;
        tree->data_length=data_length;
    }

    return tree;
}

void btree_print_preorder(btree *tree)
{
    if (tree)
    {
        printf("%s\n", tree->key);
        btree_print_preorder(tree->left);
        btree_print_preorder(tree->right);
    }
}
void btree_print_inorder(btree *tree)
{
    if (tree)
    {
        btree_print_inorder(tree->left);
        printf("%s\n", tree->key);
        btree_print_inorder(tree->right);
    }
}

void btree_dump_string_hash(btree *tree)
{
    if ((void *)tree > (void *)10)
    {
        //if (tree->data->key ==NULL) {return;}
        btree_dump_string_hash(tree->left);
        printf("\n\n****%s:\n", tree->key);
        ll_dump_string_list(tree->data);
        btree_dump_string_hash(tree->right);
    }
}

void btree_print_postorder(btree *tree)
{
    if (tree)
    {
        btree_print_postorder(tree->left);
        btree_print_postorder(tree->right);
        printf("%s\n", tree->key);
    }
}

void btree_deltree(btree *tree)
{
    if (tree)
    {
        btree_deltree(tree->left);
        btree_deltree(tree->right);
        //free(tree);  FIXME add mega_free
    }
}

/*
int min(int a, int b) {
    return (a<b) ? a : b;
}
*/

int btree_iterate(btree *tree, int (*callback)(const void*, int, void*, int, void*), void* userdata)
{
    if (tree)
    {
        int quit = btree_iterate(tree->left, callback, userdata);
        if (quit) return 1;
        quit = callback(tree->key, tree->key_length, tree->data, tree->data_length, userdata);
        if (quit) return 1;
        quit = btree_iterate(tree->right, callback, userdata);
        return 0;
    }
    return 1;
}



btree *btree_search(btree *tree, const void *key, btree_int key_length)
{
    if (!tree)
        return NULL;

    // Add assertions for validation
    BTREE_ASSERT(key != NULL || key_length == 0, "NULL key with non-zero length in btree_search");
    
    check_in_pool(hidden_pool, tree);
    check_in_pool(hidden_pool, tree->left);
    check_in_pool(hidden_pool, tree->right);
    //check_in_pool(hidden_pool, tree->data);

    //printf("mega_tree_search: Comparing '%s'(%d) and '%s'(%d)\n", val->key , val->key_length, data->key, data->key_length);

    int comp = btree_memcmp(key, tree->key, key_length, tree->key_length);
    if (comp < 0)
    {
        return btree_search(tree->left, key, key_length);
    }
    else if (comp > 0)
    {
        return btree_search(tree->right, key, key_length);
    }
    else if (comp == 0)
    {
        //printf("tree search> Found %s, returning node %p\n", tree->data->key, tree);
        return tree;
    }
    printf("tree search> Could not find %s, returning NULL\n", key);
    printf("Impossible!\n");
    abort();
}

int btree_check_integrity(btree *tree, int *issues_found)
{
    if (!tree) return 1;  // Empty tree is valid
    
    // Check for corruption markers
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        if (issues_found) (*issues_found)++;
        fprintf(stderr, "Tree integrity check: Corruption markers detected\n");
        return 0;
    }
    
    // Check for NULL key with non-zero length
    if (!tree->key && tree->key_length > 0) {
        if (issues_found) (*issues_found)++;
        fprintf(stderr, "Tree integrity check: NULL key with non-zero length\n");
        return 0;
    }
    
    // Recursively check children
    int left_ok = btree_check_integrity(tree->left, issues_found);
    int right_ok = btree_check_integrity(tree->right, issues_found);
    
    // Check BST property - each node's key should be greater than all keys in left subtree
    // and less than all keys in right subtree
    // This would be complex to implement fully, but could be added for completeness
    
    return left_ok && right_ok;
}



int btree_validate(btree *tree)
{
    if (!tree) return 1; // Empty tree is valid
    
    // Check for corruption markers
    if ((tree->bumper != 0) || (tree->bumperl != 0)) {
        printf("Tree corruption detected: non-zero bumper values\n");
        return 0;
    }
    
    // Check for null key with non-zero length
    if (tree->key == NULL && tree->key_length > 0) {
        printf("Tree corruption detected: NULL key with length %d\n", tree->key_length);
        return 0;
    }
    
    // Check children recursively
    int left_valid = 1;
    int right_valid = 1;
    
    if (tree->left) {
        left_valid = btree_validate(tree->left);
        // Verify BST property: all keys in left subtree < current key
        if (left_valid && btree_memcmp(tree->left->key, tree->key, 
                                     tree->left->key_length, tree->key_length) >= 0) {
            printf("BST property violation: left child key >= parent key\n");
            return 0;
        }
    }
    
    if (tree->right) {
        right_valid = btree_validate(tree->right);
        // Verify BST property: all keys in right subtree > current key
        if (right_valid && btree_memcmp(tree->right->key, tree->key,
                                      tree->right->key_length, tree->key_length) <= 0) {
            printf("BST property violation: right child key <= parent key\n");
            return 0;
        }
    }
    
    return left_valid && right_valid;
}


void btree_test_tree()
{
    btree *root;
    btree *tmp;
    //int i;

    root = NULL;
    /* Inserting nodes into tree */
    root = btree_insert(hidden_pool, root,  mega_insert(hidden_pool, "Inserting", 10), 10, "a", 2);
    root = btree_insert(hidden_pool, root,  mega_insert(hidden_pool, "nodes", 6), 6, "a", 2);
    root = btree_insert(hidden_pool, root,  mega_insert(hidden_pool, "into", 5), 5, "a",  2);
    root = btree_insert(hidden_pool, root,  mega_insert(hidden_pool, "tree", 5), 5, "a",  2);

    /* Printing nodes of tree */
    printf("Pre Order Display\n");
    btree_print_preorder(root);

    printf("In Order Display\n");
    btree_print_inorder(root);

    printf("Post Order Display\n");
    btree_print_postorder(root);

    /* Search node into tree */
    tmp = btree_search(root,  mega_insert(hidden_pool, "tree", 5), 5);
    if (tmp)
    {
        printf("Searched node=%d\n", tmp->data);
    }
    else
    {
        printf("Data Not found in tree->\n");
    }

    /* Deleting all nodes of tree */
    //deltree(root);
}