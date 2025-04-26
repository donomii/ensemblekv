#include "invertedindex.c"
#ifndef BINARY_TREE_PLUS_IMPORTED
struct btree_s
{
    ii_int bumper;
    void const *key;
    ii_int key_length;
    void *data;
    ii_int data_length;
    ii_int right;  // offset for right child
    ii_int left;   // offset for left child
    int bumperl;
};
typedef struct btree_s btree;

#define BTREE_DEFINED 1
#define BINARY_TREE_PLUS_IMPORTED 1
#endif