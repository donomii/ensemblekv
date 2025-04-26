#ifndef BINARY_TREE_PLUS_IMPORTED
typedef int btree_int;
struct btree_s
{
    btree_int bumper;
    void const*key;
    btree_int key_length;
    void *data;
    btree_int data_length;
    struct btree_s *right, *left;
    int bumperl;
};
typedef struct btree_s btree;

#define BTREE_DEFINED 1




#define BINARY_TREE_PLUS_IMPORTED 1
#endif

