
struct bin_tree
{
    ii_int bumper;
    mega_element *data;
    struct bin_tree *right, *left;
    int bumperl;
};
typedef struct bin_tree node;
int mymemcmp(void *a, void *b, int lena, int lenb)
{

    if (lena < lenb)
    {
        return -1;
    }
    if (lenb < lena)
    {
        return 1;
    }

    //printf("mymemcmp: same length, comparing '%s'(%d) and '%s'(%d)\n", a , lena, b, lenb);
    int res = memcmp(a, b, lena);
    //printf("memcmp returned: %d\n", res);
    return res;
}

node *mega_tree_insert(mega_pool *p, node *tree, mega_element *val)
{
    node *temp = NULL;
    if (!tree)
    {
        temp = (node *)mega_malloc(p, sizeof(node));
        temp->bumper = 0;
        temp->left = NULL;
        temp->right = NULL;
        temp->data = val;
        temp->bumperl = 0;
        return temp;
    }
    if ((tree->bumper != 0) || (tree->bumperl != 0))
    {
        printf("tree> Data corruption!\n");
        abort();
    }
    mega_element *data = tree->data;

    int comp = mymemcmp(val->key, tree->data->key, val->key_length, data->key_length);

    if (comp < 0)
    {
        tree->left = mega_tree_insert(p, tree->left, val);
    }

    else if (comp > 0)
    {
        tree->right = mega_tree_insert(p, tree->right, val);
    }

    return tree;
}

int mega_depth(node *tree, int depth)
{

    if (tree)
    {
        if ((tree->bumper != 0) || (tree->bumperl != 0)){
            printf("tree> Data corruption!\n");
            abort();
        }
        int l = mega_depth(tree->left, depth + 1);
        int r = mega_depth(tree->right, depth + 1);
        return (l > r) ? l : r;
    }
    else
    {
        return depth;
    }
}

node *mega_balance_tree(node *tree)
{

    if (!tree)
        return NULL;

    if ((tree->bumper != 0) || (tree->bumperl != 0))
    {
        printf("tree> Data corruption!\n");
        abort();
    }

    tree->left = mega_balance_tree(tree->left);
    tree->right = mega_balance_tree(tree->right);

    int l_depth = mega_depth(tree->left, 1);
    int r_depth = mega_depth(tree->right, 1);
    if (r_depth - l_depth > 1)
    {
        node *p = tree;
        node *q = p->right;
        p->right = q->left;
        q->left = p;
        tree = q;
    }

    if (r_depth - l_depth < -1)
    {
        node *q = tree;
        node *p = q->left;
        q->left = p->right;
        p->right = q;
        tree = p;
    }

    return tree;
}

node *mega_tree_set(mega_pool *p, node *tree, mega_element *val)
{
    check_in_pool(p, val);
    check_in_pool(p, val->key);
    check_in_pool(p, val->value);
    node *temp = NULL;
    if (!tree)
    {
        temp = (node *)mega_malloc(p, sizeof(node));
        temp->bumper = 0;
        temp->left = NULL;
        temp->right = NULL;
        temp->data = val;
        temp->bumperl = 0;
        return temp;
    }

    if ((tree->bumper != 0) || (tree->bumperl != 0))
    {
        printf("tree> Data corruption!\n");
        abort();
    }
    mega_element *data = tree->data;

    int comp = mymemcmp(val->key, tree->data->key, val->key_length, data->key_length);

    if (comp < 0)
    {
        tree->left = mega_tree_set(p, tree->left, val);
    }

    else if (comp > 0)
    {
        tree->right = mega_tree_set(p, tree->right, val);
    }

    else if (comp == 0)
    {
        //printf("tree set> Key %s exists, replacing value with %p\n", tree->data->key, val);
        tree->data = val;
    }

    return tree;
}

void print_preorder(node *tree)
{
    if (tree)
    {
        printf("%s\n", tree->data->key);
        print_preorder(tree->left);
        print_preorder(tree->right);
    }
}

void print_inorder(node *tree)
{
    if (tree)
    {
        print_inorder(tree->left);
        printf("%s\n", tree->data->key);
        print_inorder(tree->right);
    }
}

void dump_string_hash(node *tree)
{
    if ((void *)tree > (void *)10)
    {
        //if (tree->data->key ==NULL) {return;}
        dump_string_hash(tree->left);
        printf("\n\n****%s:\n", tree->data->key);
        ll_dump_string_list(tree->data->value);
        dump_string_hash(tree->right);
    }
}

void print_postorder(node *tree)
{
    if (tree)
    {
        print_postorder(tree->left);
        print_postorder(tree->right);
        printf("%s\n", tree->data->key);
    }
}

void deltree(node *tree)
{
    if (tree)
    {
        deltree(tree->left);
        deltree(tree->right);
        free(tree);
    }
}

/*
int min(int a, int b) {
    return (a<b) ? a : b;
}
*/
node *mega_tree_search(node *tree, mega_element *val)
{
    if (!tree)
        return NULL;

    check_in_pool(hidden_pool, tree);
    check_in_pool(hidden_pool, tree->left);
    check_in_pool(hidden_pool, tree->right);
    //check_in_pool(hidden_pool, tree->data);

    mega_element *data = tree->data;
    //printf("mega_tree_search: Comparing '%s'(%d) and '%s'(%d)\n", val->key , val->key_length, data->key, data->key_length);

    int comp = mymemcmp(val->key, tree->data->key, val->key_length, data->key_length);
    if (comp < 0)
    {
        return mega_tree_search(tree->left, val);
    }
    else if (comp > 0)
    {
        return mega_tree_search(tree->right, val);
    }
    else if (comp == 0)
    {
        //printf("tree search> Found %s, returning node %p\n", tree->data->key, tree);
        return tree;
    }
    printf("tree search> Could not find %s, returning NULL\n", val->key);
    printf("Impossible!\n");
    abort();
}

void **mega_tree_search_key(node *tree, void *key, int keylen)
{
    if (!tree)
        return NULL;

    check_in_pool(hidden_pool, tree);
    check_in_pool(hidden_pool, tree->left);
    check_in_pool(hidden_pool, tree->right);
    //check_in_pool(hidden_pool, tree->data);

    mega_element *data = tree->data;
    //printf("mega_tree_search: Comparing '%s'(%d) and '%s'(%d)\n", val->key , val->key_length, data->key, data->key_length);

    int comp = mymemcmp(key, tree->data->key, keylen, data->key_length);
    if (comp < 0)
    {
        return mega_tree_search_key(tree->left, key, keylen);
    }
    else if (comp > 0)
    {
        return mega_tree_search_key(tree->right, key, keylen);
    }
    else if (comp == 0)
    {
        //printf("tree search> Found %s, returning node %p\n", tree->data->key, tree);
        return &(data->value);
    }
    printf("tree search> Could not find %s, returning NULL\n", key);
    printf("Impossible!\n");
    abort();
}

void test_tree()
{
    node *root;
    node *tmp;
    //int i;

    root = NULL;
    /* Inserting nodes into tree */
    root = mega_tree_insert(hidden_pool, root, mega_new_element(hidden_pool, mega_insert(hidden_pool, "Inserting", 10), 10, "a", 2));
    root = mega_tree_insert(hidden_pool, root, mega_new_element(hidden_pool, mega_insert(hidden_pool, "nodes", 6), 6, "a", 2));
    root = mega_tree_insert(hidden_pool, root, mega_new_element(hidden_pool, mega_insert(hidden_pool, "into", 5), 5, "a", 2));
    root = mega_tree_insert(hidden_pool, root, mega_new_element(hidden_pool, mega_insert(hidden_pool, "tree", 5), 5, "a", 2));

    /* Printing nodes of tree */
    printf("Pre Order Display\n");
    print_preorder(root);

    printf("In Order Display\n");
    print_inorder(root);

    printf("Post Order Display\n");
    print_postorder(root);

    /* Search node into tree */
    tmp = mega_tree_search(root, mega_new_element(hidden_pool, mega_insert(hidden_pool, "tree", 5), 5, "a", 2));
    if (tmp)
    {
        printf("Searched node=%d\n", tmp->data);
    }
    else
    {
        printf("Data Not found in tree->\n");
    }

    /* Deleting all nodes of tree */
    //deltree(root);
}