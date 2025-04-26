#ifndef PATRICIA_TREE_IMPORTED
typedef int patricia_int;
struct patricia_s
{
    //patricia_int bumper;
    char key;
    void *data;
    //patricia_int data_length;
    struct patricia_s *found, *notfound;
    //int bumperl;
};
typedef struct patricia_s ptree;


#define PATRICIA_TREE_IMPORTED 1
#endif

