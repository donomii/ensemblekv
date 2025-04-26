
#include "binary_tree_plus.h"
typedef void *ptr;

/** The control struct pool */
typedef struct
{
    ii_int magic;
    ii_int size;                /**< Size of pool */
    ptr recommended_address;    /**< The address where this was originally mapped */
    ii_int btree_root;          /**< Root of our binary tree for key-value storage */
    ii_int start_free;          /**< A pointer to the start of the free space */
} mega_pool;

mega_pool *hidden_pool;

typedef struct Element
{
    ii_int bumper;
    void *key;
    ii_int key_length;
    void *value;
    ii_int value_length;
} mega_element;

void check_in_pool(mega_pool *pool, void const*data, char *msg);
ptr mega_malloc(mega_pool *pool, ii_int size);
ptr mega_insert(mega_pool *p, const void *data, ii_int len);
void kv_put(mega_pool *p, const char *key, const char *value);
const char *kv_get(mega_pool *p, const char *key);
void kv_delete(mega_pool *p, const char *key);
void kv_info(mega_pool *p);
ii_int btree_delete_node(mega_pool *p, ii_int root_offset, ii_int node_to_delete_offset);