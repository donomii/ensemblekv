#include "invertedindex.c"
#include "binary_tree_plus.h"
typedef void *ptr;
/** The control struct pool */
typedef struct
{
    ii_int size;             /**< Size of pool */
    ptr recommended_address; /**< The address where this was originally mapped */
    btree *table_of_contents; /**< A convenient registry so users can find their data after a reboot */
    char *stringbank_word;   //**< The word "stringbank".  It is automatically placed in the megapool when the pool is created. */
    ii_int start_free;       /**< A pointer to the start of the free space */
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


void check_in_pool(mega_pool *pool, void const*data);
ptr mega_malloc(mega_pool *pool, ii_int size);
ptr mega_insert(mega_pool *p, const void *data, ii_int len);