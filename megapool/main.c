#include "binary_tree_plus.h"
#include "megapool.h"
#include "megapool.c"

int ent_search_command_line_switch(int argc, char *args[], char *searchString) {
    int i;
    for (i = 1; i < argc; i++) {
        if (strcmp(args[i], searchString) == 0) {
            return (i);
        }
    }
    return (0);
}

int main(int argc, char *args[])
{
    ii_int pos = 0;
    ii_int size = -1;
    char *filename = "default.mmap";

    if ((ent_search_command_line_switch(argc, args, "--help")) || (ent_search_command_line_switch(argc, args, "-h"))) {
        printf("Usage: %s [options]\n", args[0]);
        printf("Options:\n");
        printf("  --help, -h            Show this help message\n");
        printf("  --file <filename>     Specify the file to use\n");
        printf("  --verbose             Print extra information\n");
        printf("  --debug               Print debugging information\n");
        printf("  --size <size>         Force size of the pool\n");
        printf("  --info                Show information about the pool\n");
        printf("  --dump                Dump all key-value pairs\n");
        printf("  --put <key> <value>   Add or update a key-value pair\n");
        printf("  --get <key>           Get a value by key\n");
        printf("  --delete <key>        Delete a key-value pair\n");
        exit(0);
    }

    pos = ent_search_command_line_switch(argc, args, "--file") + 1;
    if (pos - 1)
    {
        filename = args[pos];
        printf("Using file %s\n", filename);
    }

    if (ent_search_command_line_switch(argc, args, "--verbose"))
    {
        verbose = 1;
        v("Printing extra information\n");
    }
    if (ent_search_command_line_switch(argc, args, "--debug"))
    {
        debug = 1;
        d("Printing debugging information\n");
    }

    pos = ent_search_command_line_switch(argc, args, "--size") + 1;
    if (pos - 1)
    {
        size = atoll(args[pos]);
        printf("Forcing size to %lld\n", size);
        printf("Resizing and mapping %s\n", filename);
        mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(filename, size, (ptr)address_base);
        pool->size = size;
        ii_munmap(pool, pool->size);
        printf("Complete!\n");
        exit(0);
    }
    
    mega_pool *p = start_megapool(filename, size);

    if (ent_search_command_line_switch(argc, args, "--info"))
    {
        kv_info(p);
    }

    // Process put command
    pos = ent_search_command_line_switch(argc, args, "--put") + 1;
    if (pos - 1 && pos + 1 < argc)
    {
        const char *key = args[pos];
        const char *value = args[pos + 1];
        kv_put(p, key, value);
        printf("Added: %s -> %s\n", key, value);
    }

    // Process get command
    pos = ent_search_command_line_switch(argc, args, "--get") + 1;
    if (pos - 1)
    {
        const char *key = args[pos];
        const char *value = kv_get(p, key);
        if (value) {
            printf("Value for '%s': %s\n", key, value);
        } else {
            printf("Key '%s' not found\n", key);
        }
    }

    // Process delete command
    pos = ent_search_command_line_switch(argc, args, "--delete") + 1;
    if (pos - 1)
    {
        const char *key = args[pos];
        kv_delete(p, key);
        printf("Deleted key: %s\n", key);
    }
    
    // Process dump command
    if (ent_search_command_line_switch(argc, args, "--dump"))
    {
        printf("Dumping all key-value pairs:\n");
        kv_info(p);
        btree_iterate(p, p->btree_root, print_entry, NULL);
    }

    if (!btree_validate(p, p->btree_root))
    {
        printf("Tree is corrupted!\n");
        exit(1);
    }

    msync(p, p->size, MS_SYNC);
    ii_munmap(p, p->size);
    printf("Complete!\n");
    exit(0);
}