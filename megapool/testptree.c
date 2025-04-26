 #include "megapool.h"
 #include "megapool.c"

int main(int argc, char *args[])
{
    char* filename = "default.mmap";
    hidden_pool = mega_create_or_resize_pool(filename, -1);
    mega_pool *p = hidden_pool;
    patricia_test_tree();
}
