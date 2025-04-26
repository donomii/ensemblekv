#include "patricia.h"



ptree* patricia_insert(mega_pool *p, ptree* tree, const char *key, int keylen, int keypos, void *data, patricia_int data_length )
{
    
     //We reached the end of the key
       if (keypos>=keylen) {
           //printf("End of key\n");
        return NULL;
    }
    
    
    ptree*temp = NULL;
    if (!tree)
    {
        temp = (ptree*)mega_malloc(p, sizeof(ptree));
        
        temp->key = key[keypos];
        temp->data = NULL;
        //temp->data_length=0;
      
        //printf("Adding node %c from key %s at pos  %lld \n", temp->key, key, keypos);
        temp->found = patricia_insert(p, temp->found, key, keylen, keypos+1, data, data_length);
        temp->notfound=NULL;
        if (keypos==keylen-1) {
            temp->data = data;
        }
         
        return temp;
    }
    
        if (keypos==keylen-1) {
            d("Key already exists in tree, will not replace!\n");
        }
         

//Match
    if (key[keypos]==tree->key)
        {tree->found    = patricia_insert(p, tree->found, key, keylen, keypos+1, data, data_length);}
//Everything else that isn't a match
    else 
        {tree->notfound = patricia_insert(p, tree->notfound, key, keylen, keypos, data, data_length);}

    return tree;
}

int patricia_depth(ptree*tree, int depth)
{

    if (tree)
    {
        int l = patricia_depth(tree->found, depth + 1);
        int r = patricia_depth(tree->notfound, depth + 1);
        return (l > r) ? l : r;
    }
    else
    {
        return depth;
    }
}


ptree* patricia_set(mega_pool *p, ptree* tree, const char *key, int keylen, int keypos, void *data, patricia_int data_length )
{
    
     //We reached the end of the key
       if (keypos>=keylen) {
           //printf("End of key\n");
        return NULL;
    }
    
    
    ptree*temp = NULL;
    if (!tree)
    {
        temp = (ptree*)mega_malloc(p, sizeof(ptree));
        
        temp->key = key[keypos];
        temp->data = NULL;
        //temp->data_length=0;
      
        //printf("Adding node %c from key %s at pos  %lld \n", temp->key, key, keypos);
        temp->found = patricia_set(p, temp->found, key, keylen, keypos+1, data, data_length);
        temp->notfound=NULL;
        if (keypos==keylen-1) {
            temp->data = data;
            //temp->data_length=data_length;
        }
         
        return temp;
    }
    

        if (keypos==keylen-1) {
            d("Patricia found key, updating\n");
            tree->data = data;
            //temp->data_length=data_length;
            return tree;
        }
         
        


//Match
    if (key[keypos]==tree->key)
        {tree->found    = patricia_set(p, tree->found, key, keylen, keypos+1, data, data_length);}
//Everything else that isn't a match
    else 
        {tree->notfound = patricia_set(p, tree->notfound, key, keylen, keypos, data, data_length);}

    return tree;
}



//Count the number of nodes in the tree
int patricia_count(ptree*tree)
{
    if (tree)
    {
        return patricia_count(tree->found) + patricia_count(tree->notfound);
    } else {
        return 1 ;
    }
    return 0;
}

int dump_n = 0;
void patricia_iterate(ptree*tree, int (*callback)(char, int, void*, int, void*), void* userdata)
{
    if (tree)
    {
        callback(tree->key, -1,tree->data, -1,userdata);
        //printf("%c", tree->key);
        dump_n=1;
        patricia_iterate(tree->found,callback,userdata);
        if (tree->notfound) {
        dump_n=0;
        }
        patricia_iterate(tree->notfound,callback,userdata);
    }
}



void patricia_dump(ptree*tree, int indent)
{
    if (tree)
    {
        printf("%c", tree->key);
        dump_n=1;
        patricia_dump(tree->found, indent+1);
        if (tree->notfound) {
        printf("\n%*c", indent, ' ');
        dump_n=0;
        }
        patricia_dump(tree->notfound, indent);
    }
}

void patricia_dump2(ptree*tree, int indent)
{
    if (tree)
    {
        printf("%c", tree->key);
        dump_n=1;
        patricia_dump2(tree->notfound, indent+1);
        if (tree->notfound) {
        printf("\n%*c", indent, ' ');
        dump_n=0;
        }
        patricia_dump2(tree->found, indent);
    }
}


void** patricia_search( ptree* tree, char const* key, int keylen, int keypos )
{
     //We reached the end of the key
    if (!tree) {
        return NULL;
    }
    


    //printf("Comparing %c and %c\n",key[keypos],tree->key); 
    if (key[keypos]==tree->key) {
        //Match
        if (keypos>=keylen-1) {
            //printf("End of key\n");
            
            return &(tree->data);
            //return 1;
        }
        
        return  patricia_search( tree->found, key, keylen, keypos+1);
    } else {
        //Everything else that might be a match
        return patricia_search( tree->notfound, key, keylen, keypos);
    }
    return NULL;
}
/*
ptree*patricia_search(ptree*tree, void *key, patricia_int key_length)
{
    if (!tree)
        return NULL;

    check_in_pool(hidden_pool, tree);
    check_in_pool(hidden_pool, tree->left);
    check_in_pool(hidden_pool, tree->right);
    //check_in_pool(hidden_pool, tree->data);

    //printf("mega_tree_search: Comparing '%s'(%d) and '%s'(%d)\n", val->key , val->key_length, data->key, data->key_length);

    int comp = patricia_memcmp(key, tree->key, key_length, tree->key_length);
    if (comp < 0)
    {
        return patricia_search(tree->left, key, key_length);
    }
    else if (comp > 0)
    {
        return patricia_search(tree->right, key, key_length);
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
*/

void patricia_test_tree()
{
    ptree*root;
    void*tmp;
    //int i;

    root = NULL;
    printf("Inserting nodes into tree\n");
    root = patricia_insert(hidden_pool, root,  mega_insert(hidden_pool, "Inserting", 10),10, 0, "a", 2);
    root = patricia_insert(hidden_pool, root,  mega_insert(hidden_pool, "nodes", 6),6, 0, "a", 2);
    root = patricia_insert(hidden_pool, root,  mega_insert(hidden_pool, "into", 5),5, 0, "a",  2);
    root = patricia_insert(hidden_pool, root,  mega_insert(hidden_pool, "tree", 5),5, 0, "a",  2);

    /* Printing nodes of tree */
    printf("Dumping tree contents\n");
    patricia_dump(root,0);


    /* Search node into tree */
    printf("Searching for 'tree' in patricia tree\n");
    tmp = patricia_search(root,  mega_insert(hidden_pool, "tree", 5), 4,0);
    if (tmp)
    {
        printf("Found entry=%s\n", "tree");
    }
    else
    {
        printf("Term Not found in tree\n");
    }
    
    printf("Searching for 'tref' in patricia tree\n");
    tmp = patricia_search(root,  mega_insert(hidden_pool, "tref", 5), 5,0);
    if (tmp)
    {
        printf("Found entry=%s\n", "tref");
    }
    else
    {
        printf("Term Not found in tree\n");
    }
    
    

    /* Deleting all nodes of tree */
    //deltree(root);
}
