#ifndef LINKED_LIST_IMPORTED
 typedef struct ll_pair_ss
{
    void *car;
    struct ll_pair_ss *cdr;
} ll_pair_s;

typedef ll_pair_s *ll_pair;

void ll_dump_string_list(ll_pair l);
#define LINKED_LIST_IMPORTED 1
#endif