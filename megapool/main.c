 #include "megapool.h"
 #include "megapool.c"

 

void dump_toc_btree(char* dbname){
        btree *db = toc(dbname);
        if (db)
        {
            printf("%s depth: %d\n", dbname, btree_depth(db, 1));
            printf("Dumping %s:\n", dbname);
            btree_print_inorder(db);
        }
        else
        {
            printf("Cannot find database: '%s'  \n", dbname);
            //exit(1);
        }
}


void display_ptree(char* name) {
        ptree *hashes = toc(name);
        if (hashes)
        {
            printf("%s depth: %d\n",name, patricia_depth(hashes, 1));
            printf("Dumping %s:\n",name);
            patricia_dump(hashes,0);
        }
        else
        {
            printf("Cannot find %s\n",name);
            exit(1);
        }

}

//print bytes as hex
void print_hex(unsigned char*buf, int len) {
    int i;
    for(i=0; i<len; i++)
        printf("%02x",buf[i]);
}

void printSha256(void* hash) {
 print_hex(hash, 32);
}

void printMurmur(void* hash) {
 print_hex(hash, 16);
}
int iter_btree(char key, int keylen, void* data, int datalen, void* userdata){
    if (data){
    btree* btr = (btree *)data;
    //printMurmur(key);
    //printf("%s:\n",key);
    if (btree_count(btr)>1){
    printf("=========");
    printSha256(data);
    printf("\n");
    btree_print_inorder(btr);
    }
    }
    return 0;
}

int main(int argc, char *args[])
{
    ii_int pos = 0;
    ii_int size = -1;
    char *scandir = NULL;
    char *filename = "default.mmap";

    if ((ent_search_command_line_switch(argc, args, "--help")) || (ent_search_command_line_switch(argc, args, "-h"))) {
        printf("Usage: %s [options]\n", args[0]);
        printf("Options:\n");
        printf("  --help, -h            Show this help message\n");
        printf("  --file <filename>     Specify the file to use\n");
        printf("  --scan <directory>    Recursively scan a directory\n");
        printf("  --scan-contents       Also scan file contents\n");
        printf("  --filter <string>     Only scan files containing this string\n");
        printf("  --verbose             Print extra information\n");
        printf("  --debug               Print debugging information\n");
        printf("  --size <size>         Force size of the pool\n");
        printf("  --info                Show information about the pool\n");
        printf("  --cleanup             Clean up the pool\n");
        printf("  --show-duplicates     Show duplicate entries\n");
        printf("  --dump                Dump the contents of the pool\n");
        exit(0);
    }

    pos = ent_search_command_line_switch(argc, args, "--file") + 1;
    if (pos - 1)
    {
        filename = args[pos];
        printf("Using file %s\n", filename);
    }



    pos = ent_search_command_line_switch(argc, args, "--scan") + 1;
    if (pos - 1)
    {
        scandir = args[pos];
        printf("Recursively scanning directory %s\n", scandir);
    }

        
    if (pos = ent_search_command_line_switch(argc, args, "--scan-contents")) {
        indexContents=1;
        printf("Also scanning file contents\n");
    }

    pos = ent_search_command_line_switch(argc, args, "--filter") + 1;
    if (pos - 1)
    {
        fileFilter = args[pos];
        printf("Only scanning files that contain %s\n", fileFilter);
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
        printf("Forcing size to  %lld \n", size);
        printf("Resizing and mapping %s\n", filename);
        mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(filename, size, (ptr)address_base);
        pool->size = size;
        ii_munmap(pool, pool->size);
        printf("Complete!\n");
        exit(0);
    }
    
    mega_pool *p = start_megapool(filename,size);

    if (ent_search_command_line_switch(argc, args, "--info"))
    {
        info(hidden_pool);
        exit(0);
    }

    
    

    btree* stringbank = toc("stringbank");
    if (ent_search_command_line_switch(argc, args, "--cleanup"))
    {
        info(hidden_pool);
        printf("Balancing...\n");
        btree* files = toc("files");

        for (int i=0; i<100; i++) {
            stringbank = btree_balance(stringbank);
            files = btree_balance(files);
            btree* maps = toc("tags");
            btree_balance(maps);
            btree* points = toc("points");
            btree_balance(points);
        }
        btree_set(p, p->table_of_contents, p->stringbank_word, 11, stringbank, 6);
        btree_set(p, p->table_of_contents, add_to_stringbank(p, "files"), 6, files, 6);
        info(hidden_pool);
        exit(0);
    }

    // printf("Allocating memory...\n");
    // ptr mem = mega_malloc(p, 111);
    // snprintf(mem, 100, "%s\n", "Allocation successful!");
    // printf("%s", mem);

    // test_tree();


    if (ent_search_command_line_switch(argc, args, "--show-duplicates")) {
        ptree* h2f = toc("HashesToFiles");
        patricia_iterate(h2f, iter_btree, NULL );
        exit(0);
    }

    if (ent_search_command_line_switch(argc, args, "--dump"))
    {

        if (p->table_of_contents != NULL)
        {
            printf("Datastructures registered in the Table of Contents\n");
            btree_print_inorder(p->table_of_contents);
        }
        else
        {
            printf("Cannot find table of contents\n");
            exit(1);
        }



        display_ptree("files");
         display_ptree("stringbank");
        display_ptree("HashesToFiles");
         display_ptree("FilesToHashes");
        
        ptree *patricia = toc("patricia");
        if (patricia)
        {
            printf("patricia depth: %d\n", patricia_depth(patricia, 1));
            printf("Dumping patricia tree:\n");
            patricia_dump2(patricia,0);
            printf("\n");
        }

        dump_toc_btree("points");
        dump_toc_btree("tags");
        exit(0);
    }

    if (scandir)
    {
        printf("Adding files\n");
        ptree *files = toc("files");
        d("Searched for files\n");
        //printf("files depth: %d\n", patricia_depth(files, 1));
        toc_set(p, "files", listdir(hidden_pool, files, scandir, 0));

        printf("Finished adding files\n");
        //patricia_dump(patr,0);
    }

    pos = ent_search_command_line_switch(argc, args, "--search") + 1;
    if (pos - 1)
    {
        doSearch(p);
    }

    ii_munmap(hidden_pool, p->size);
    printf("Complete!\n");
    exit(0);
}
