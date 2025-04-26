
int debug = 0;
//
// Debug printf
void d(const char *fmt, ...) {
  if (debug) {
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
  }
}

#include "getline.c"
#include "linked_list.h"
#include "patricia.c"
#include "tokeniser.c"
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

// #include "binary_tree.c"
#include "binary_tree_plus.c"
#include "linked_list.c"

#include "murmur/murmur3.c"
#include "murmur/murmur3.h"
#include "sha256/sha256.c"

void *address_base = (void *)0x1000000000;
int verbose = 0;
char *fileFilter = NULL;
int indexContents = 0;

void v(const char *fmt, ...) {
  if (verbose) {
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
  }
}

btree *btree_insert(mega_pool *p, btree *tree, void *key, btree_int key_length,
                    void *data, btree_int data_length);
void ll_dump_string_list(ll_pair l);

// Open an existing megapool file
mega_pool *mega_open_pool(char *filename) {
  mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
      filename, ii_get_file_length(filename), (ptr)address_base);
  return pool;
}

// Allocate new memory inside the pool.  The memory is permanent, any data you
// write there will be there, at the same address, next time you start megapool
ptr mega_malloc(mega_pool *pool, ii_int size) {
  if (size < 1) {
    abort();
  }
  char *p = (char *)pool;
  char *new_alloc = p + pool->start_free;
  pool->start_free = pool->start_free + size;
  if (pool->size <= pool->start_free) {
    printf("Out of memory while trying to allocated megapool memory!  Extend "
           "the megapool!\n");
    exit(1);
  }
  // printf("Allocated %p (%d%%).  Requested  %lld , got  %lld . \n", new_alloc,
  // pool->start_free*100/(pool->size*100), size, pool->start_free-old);
  return (void *)new_alloc;
}

// Remove
mega_element *mega_new_element(mega_pool *mp, void *key, ii_int key_length,
                               void *value, ii_int value_length) {
  mega_element *me = mega_malloc(mp, sizeof(mega_element));
  me->bumper = 0;
  me->key = key;
  me->key_length = key_length;
  me->value = value;
  me->value_length = value_length;
  return me;
}

// Remove
mega_element *mega_new_element_outside(void *key, ii_int key_length,
                                       void *value, ii_int value_length) {
  mega_element *me = malloc(sizeof(mega_element));
  me->key = key;
  me->key_length = key_length;
  me->value = value;
  me->value_length = value_length;
  return me;
}

// Allocate some memory inside the pool, and copy len bytes from data into the
// allocated memory.  The newly allocated pool memory is permanent, and will be
// available at the some address the next time megapool is started.
ptr mega_insert(mega_pool *p, const void *data, ii_int len) {
  ptr e = mega_malloc(p, len);
  memcpy(e, data, len);
  return e;
}

// Setup a new pool file
mega_pool *mega_new_pool(char *filename, ii_int size) {
  mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
      filename, size, (ptr)address_base);
  if (pool == NULL) {
    printf("Unable to initialise %s to size %llu\n", filename, size);
  }
  pool->start_free = sizeof(mega_pool);
  pool->size = size;
  pool->recommended_address = pool;
  pool->table_of_contents = NULL;
  pool->table_of_contents =
      btree_insert(pool, pool->table_of_contents, mega_insert(pool, "TOC", 4),
                   4, mega_insert(pool, "TOC", 4), 4);
  pool->stringbank_word = mega_insert(pool, "stringbank", 11);

  hidden_pool = pool;
  return pool;
}

// Open the file on disk and resize as needed.  Set size to -1 to skip resizing
// and just open it.
mega_pool *mega_create_or_resize_pool(char *filename, ii_int size) {
  printf("Char * is %d bytes long\n", sizeof(char *));
  printf("Void * is %d bytes long\n", sizeof(void *));
  printf("ii_int is %d bytes long\n", sizeof(ii_int));
  ii_int flen = ii_get_file_length(filename);
  if (flen == 0) {
    if (size < 1) {
      size = 100000000;
    }
    return mega_new_pool(filename, size);
  } else {
    if (size == -1) {
      size = flen;
    }
    // resize
    if (size != flen) {
      printf("Resizing and mapping");
      mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
          filename, size, (ptr)address_base);
      pool->size = size;
      return pool;
    } else {
      return mega_open_pool(filename);
    }
  }
}

// FIXME
int mega_free(mega_pool *pool, ptr to_be_freed) { return 0; }

// Current pool size (on disk and in memory)
int mega_current_size(mega_pool *pool) { return pool->size; }

// Confirm pointer is inside pool
void check_in_pool(mega_pool *pool, void const *data) {
  if (data == NULL) {
    return;
  }
  if (data < (void *)pool) {
    printf("Pointer %p is lower than megapool (%p)\n", data, pool);
    btree *p = NULL;
    // p->data;
    printf("%s", p->data);
    exit(1);
    abort();
  }
  if (data > ((void *)pool + pool->size)) {
    printf("Pointer %p is outside megapool (%p - %p)\n", data, pool,
           (void *)pool + pool->size);
    btree *p = NULL;
    // p->data;
    printf("%s", p->data);
    exit(1);
    abort();
  }
}

void *toc(char *key) {
  btree *r = btree_search(hidden_pool->table_of_contents, key, strlen(key) + 1);
  if (r) {
    return r->data;
  }
  return NULL;
}
char *add_to_stringbank(mega_pool *p, const char *newString) {

  // Get the stringbank
  ptree *stringbank = toc("stringbank");

  // Lookup the string
  char **x = (char **)patricia_search(stringbank, (char *)newString,
                                      strlen(newString) + 1, 0);
  if (x) {
    char *data = *x;
    // String exists in stringbank.  Return it
    if (strlen(newString) != strlen(data)) {
      printf("Searched for %s but got %s!", newString, data);
      abort();
    }
    return data;
  }

  // String doesn't exist, insert it into pool
  char *interned_string = mega_insert(p, newString, strlen(newString) + 1);

  // Register it in the stringbank
  ptree *newStringbank =
      patricia_insert(p, stringbank, interned_string, strlen(newString) + 1, 0,
                      interned_string, strlen(newString) + 1);

  // balance it
  // newStringbank = btree_balance(newStringbank);
  // newStringbank = btree_balance(newStringbank);

  // Save the stringbank
  p->table_of_contents = btree_set(p, p->table_of_contents, p->stringbank_word,
                                   11, newStringbank, 6);

  return interned_string;
}

void toc_set(mega_pool *p, char *key, void *pt) {
  p->table_of_contents = btree_set(
      p, p->table_of_contents, add_to_stringbank(p, key), strlen(key) + 1, pt,
      // insertLines(p,current),
      -1);
}

ptree *insert_hash(mega_pool *p, ptree *hash, void const *key, ii_int keylen,
                   void const *data, ii_int datalen) {
  d("Insert hash %s, %d\n", key, keylen);
  // check_in_pool(p, key);

  if (key == NULL) {
    return hash;
  }

  if (keylen < 1) {
    return hash;
  }
  /*      if (keylen > 40)
    {
        v("Oversize key, refusing to store\n");
        return hash;
    }*/
  if (key == NULL) {
    return hash;
  }

  key = add_to_stringbank(p, key);
  data = add_to_stringbank(p, data);
  check_in_pool(p, data);
  check_in_pool(p, key);

  d("\n\nInserting %s -> %s\n", key, data);

  ptree *patr = hash;
  btree **u = (btree **)patricia_search(patr, key, strlen(key) + 1, 0);
  btree *uri_list = u ? *u : NULL;

  if (uri_list == NULL) {
    d("Key not found, inserting\n");
    uri_list = btree_set(p, uri_list, data, datalen, (void *)data, datalen);
    patr = patricia_insert(p, patr, key, strlen(key) + 1, 0, uri_list,
                           sizeof(btree *));
  } else {
    d("Key found, updating\n");
    uri_list = btree_set(p, uri_list, data, datalen, (void *)data, datalen);
    patr = patricia_set(p, patr, key, strlen(key) + 1, 0, uri_list,
                        sizeof(btree *));
  }

  toc_set(p, "files", patr); // FIXME remove

  // mega_element *newElem = mega_new_element_outside(key, keylen, NULL, 0);
  //  newElem->key = key;
  //  newElem->key_length = keylen;
  // d("\n\nSearching for %s \n", key);
  // btree *current = patricia_search(hash, key, keylen,0);
  // d("Found %p\n", current);
  // void *oldData = current != NULL ? current->data : NULL;

  // printf("Old list is:\n");
  // ll_dump_string_list(oldData);

  // hash = btree_set(p, hash, key, keylen, ll_cons(data, oldData),
  // sizeof(ll_pair));
  //  printf("Inserted %s -> \n", key);
  //  ll_dump_string_list(insElem->value);
  // free(newElem);
  return patr;
}

ptree *insert_tags(mega_pool *p, ptree *hash, char const *input,
                   const char *name) {
  d("Insert tags %s\n", name);
  char *context, *iter;

  d("****Breaking up %s\n", input);

  // char * data  = mega_insert(p, input, strlen(name)+1);
  // char *data = add_to_stringbank(p, name); // mega_insert(p, input,
  // strlen(name)+1);
  // printf("Found %s in stringbank\n",data);
  char *in_buff = malloc(strlen(input) + 1);
  memcpy(in_buff, input, strlen(input) + 1);

  d("Breaking %s\n", in_buff);

  FOR_EACH_TOKEN(context, iter, in_buff, "/\\\n ._-()[]")
  hash = insert_hash(p, hash, iter, strlen(iter), name, strlen(name));

  free(in_buff);

  return (hash);
}
ptree *insertLines(mega_pool *p, ptree *hash) {
  d("insertlines\n");
  char *b = NULL;
  ssize_t characters;
  size_t bufsize;
  bufsize = 1024;

  for (;;) {
    b = (void *)p + p->start_free;
    characters = getline(&b, &bufsize, stdin);

    if ((characters == -1) || characters < 2) {
      printf("Finished load!\n");
      return (hash);
    }
    mega_malloc(p, characters + 2);
    // void* added = mega_insert(b, characters);
    if (characters > 1) { // Skip blank line
      hash = insert_tags(p, hash, b, b);
      // p->table_of_contents=mega_tree_insert(p->table_of_contents,
      // mega_new_element(p,added, characters, added, characters)); printf("%s,
      // %d\n",buffer, characters);
    }
  }
}

#include "readall.c"

uint64_t filesize(char *filename) {
    int fd = ii_get_readwrite_fd(filename);
    if (fd < 0) {
        printf("Cannot open file: '%s'\n", filename);
        return 0;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1) {
        printf("lseek failed for '%s'\n", filename);
        close(fd);
        return 0;
    }

    close(fd);
    return (uint64_t)size;
}



void *LoadFile(const char *filename) {
    int fd = ii_get_readwrite_fd(filename);
    if (fd < 0) {
        printf("Cannot open file: '%s'\n", filename);
        return NULL;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1) {
        printf("lseek failed for file: '%s'\n", filename);
        close(fd);
        return NULL;
    }

    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        printf("lseek reset failed for file: '%s'\n", filename);
        close(fd);
        return NULL;
    }

    void *data = malloc(size);
    if (!data) {
        printf("Cannot allocate memory for file: '%s'\n", filename);
        close(fd);
        return NULL;
    }

    ssize_t bytes_read = read(fd, data, size);
    if (bytes_read != size) {
        printf("read failed for file: '%s'\n", filename);
        free(data);
        close(fd);
        return NULL;
    }

    close(fd);
    return data;
}

// Calculate the murmur hash of a file
char *FileSHA256Hash(const char *path) {

  void *data = NULL;
  uint64_t size = ii_get_file_length(path);
  printf("Sha256 hashing %s of size %llu\n", path, size);
  if (size > 0) {
#ifdef II_WINDOWS
    HANDLE fh = ii_get_readonly_fd(path);
    if (fh == 0) {
      return NULL;
    }
    data = ii_mmap_rw(size, fh, 0, 0, NULL);
    CloseHandle(fh);
#else
    int fh = ii_get_readonly_fd(path);
    if (fh == -1) {
      return NULL;
    }
    data = ii_mmap_rw(size, fh, 0, 0, NULL);
    close(fh);
#endif

    if (data == NULL) {
      return NULL;
    }

    // printf("Mapped %s to %p\n", path, data);
  } else {
    printf("Skipping %s because it is empty\n");
  }
  SHA256_HASH *hash = calloc(sizeof(SHA256_HASH) + 1, 1);

  Sha256Calculate(data, // [in]
                  size, // [in]
                  hash);
  // MurmurHash3_x86_128(data,size,0,hash);
  printf("\n%s:", path);

  uint32_t *t = (uint32_t *)hash;

  printf("%08x%08x%08x%08x", t[0], t[1], t[2], t[3]);

  printf("\n");
  // free(data);
  ii_munmap(data, size);
  return (char *)hash;
}

// Calculate the murmur hash of a file
char *FileMurmurHash(char *path) {

  void *data = NULL;
  uint64_t size = filesize(path);
  printf("Murmur hashing %s of size %llu\n", path, size);
  if (size > 0) {
#ifdef II_WINDOWS
    HANDLE fh = ii_get_readonly_fd(path);
    if (fh == 0) {
      return NULL;
    }
    data = ii_mmap_rw(size, fh, 0, 0, NULL);
    CloseHandle(fh);
#else
    int fh = ii_get_readonly_fd(path);
    if (fh == -1) {
      return NULL;
    }
    data = ii_mmap_rw(size, fh, 0, 0, NULL);
    close(fh);
#endif
  } else {
    printf("Skipping %s because it is empty\n");
  }
  SHA256_HASH *hash = calloc(sizeof(SHA256_HASH) + 1, 1);

  MurmurHash3_x86_128(data, size, 0, hash);
  printf("\n%s:", path);

  uint32_t *t = (uint32_t *)hash;

  printf("%08x%08x%08x%08x", t[0], t[1], t[2], t[3]);

  printf("\n");
  // free(data);
  ii_munmap(data, size);
  return (char *)hash;
}

ptree *listdir(mega_pool *p, ptree *hash, const char *name, ii_int indent) {
  DIR *dir;
  struct dirent *entry;
  size_t size;
  char *data;
  d("Listdir %s\n", name);

  if (!(dir = opendir(name))) {
    if (!fileFilter || fileFilter && strstr(name, fileFilter)) {
      v("Indexing file %s\n", name);
      ptree *filesToHashes = toc("FilesToHashes");
      void **exists = patricia_search(filesToHashes, name, strlen(name) + 1, 0);
      // printf("Searching for %s in fileshashes store\n", name);
      if (exists == NULL) {
        ptree *hashesToFiles = toc("HashesToFiles");
        char *murmur = FileSHA256Hash(name);
        if (murmur != NULL) {
          filesToHashes =
              insert_hash(p, filesToHashes, name, strlen(name) + 1, murmur, 33);
          hashesToFiles =
              insert_hash(p, hashesToFiles, murmur, 33, name, strlen(name) + 1);
          toc_set(p, "FilesToHashes", filesToHashes);
          toc_set(p, "HashesToFiles", hashesToFiles);
          free(murmur);
        }
      } else {
        printf("%s is indexed, skipping\n", name);
      }
      hash = insert_tags(p, hash, name, name);

      if (indexContents) {
        int f = ii_get_readonly_fd(name);
        int failure = readall(f, &data, &size);
        if (!failure) {
          if (size > 0) {
            d("Read  %lld  bytes from : %s\n", size, name);
            hash = insert_tags(p, hash, data, name);
            free(data);
          }
          close(f);
          v("Indexed file contents %s\n", name);
        }
      }

      return hash;
    }
  }

  while ((entry = readdir(dir)) != NULL) {
    char path[1024];
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    snprintf(path, sizeof(path), "%s/%s", name, entry->d_name);
    d("%*s[%s]\n", indent, "", entry->d_name);
    hash = listdir(p, hash, path, indent + 2);
    // toc_set(p, "files", hash); //FIXME DO I want to record partial loads?
  }
  closedir(dir);
  return hash;
}

int ent_search_command_line_switch(int argc, char *args[], char *searchString) {
  int i;
#ifdef DEBUG
  // printf("Searching for %s in arglist\n", searchString);
#endif
  for (i = 1; i < argc; i++) /* Skip argv[0] (program name). */
  {
/*
 * Use the 'strcmp' function to compare the argv values
 * to a string of your choice (here, it's the optional
 * argument "-q").  When strcmp returns 0, it means that the
 * two strings are identical.
 */
#ifdef DEBUG
    // printf("Comparing %s, %s\n", ent_argv[i], searchString);
#endif
    if (strcmp(args[i], searchString) == 0) /* Process optional arguments. */
    {
      return (i);
    }
  }
  return (0);
}

int (*iterFunc_t)(void *, int, void *, int, void *);
int dumpFiles(const void *key, int keylen, void *data, int datalen,
              void *userdata) {
  d("Searching in %s %s\n", userdata, key);
  ptree *f2h = toc(userdata);
  d("%s: %p\n", userdata, f2h);
  btree *hashes = *patricia_search(f2h, key, strlen(key) + 1, 0);
  d("Found btree: %p\n", hashes);
  if (hashes) {
    btree_print_inorder(hashes);
  }
  return 0;
}
int mySearch(const void *key, int keylen, void *data, int datalen,
             void *userdata) {
  ll_pair searchList = userdata;
  ii_int matchesAll = 1;
  for (ll_pair s = searchList; s != NULL; s = s->cdr) {
    // printf("Searching for %s in %s\n", s->car, a->car);
    char *searchTerm = s->car;

    ptree *files = toc("files");
    btree **aanswerList =
        (btree **)patricia_search(files, searchTerm, strlen(searchTerm) + 1, 0);

    if (aanswerList) {
      btree *answerList = *aanswerList;
      if (btree_search(answerList, key, keylen) == NULL) {
        matchesAll = 0;
      }
    } else {
      matchesAll = 0;
    }
  }
  if (matchesAll == 1) {
    // printf("%s\n", key);
    // ptree* h2f = toc("HashesToFiles");
    d("Searching for hash of %s\n", key);
    ptree *f2h = toc("FilesToHashes");
    d("FilestoHashes: %p\n", f2h);
    btree *hashes = *patricia_search(f2h, key, strlen(key) + 1, 0);
    d("Found btree: %p\n", hashes);
    if (hashes) {
      btree_print_inorder(hashes);
      btree_iterate(hashes, dumpFiles, "HashesToFiles");
    }
  }
  return 0;
}
void search_files(mega_pool *p, ll_pair searchList) {

  ptree *files = toc("files");

  if (!files) {
    printf("Files empty!\n");
    abort();
  }

  char *key = searchList->car;
  btree **aanswerList =
      (btree **)patricia_search(files, key, strlen(key) + 1, 0);
  if (aanswerList) {
    btree *answerList = *aanswerList;
    d("Answer list string dump: ");
    if (debug) {
      btree_print_inorder(answerList);
    }
    d("Answer list direct dump: ");

    btree_iterate(answerList, &mySearch, searchList);
  } else {
    printf("%s not found\n", key);
  }
}
void doSearch(mega_pool *p) {
  char buffer[1024];
  char *b = (char *)&buffer;
  ssize_t characters;
  size_t bufsize;
  bufsize = 1024;
  ll_pair search = NULL;
  for (;;) {
    printf("Search>");
    characters = getline(&b, &bufsize, stdin);

    if ((characters == -1) || characters < 2) {
      printf("Finished search!\n");
      return;
    }
    if (characters > 1) {         // Skip blank line
      buffer[characters - 1] = 0; // Remove LF
      search = ll_cons(add_to_stringbank(p, b), search);
      printf("Searching for:\n");
      ll_dump_string_list(search);
      printf("-----\n");
      search_files(p, search);
    }
  }
}

void info(mega_pool *p) {

  ptree *stringbank = toc("stringbank");
  printf("String bank: %p\n", stringbank);
  printf("stringbank depth:  %lld , count  %lld \n",
         patricia_depth(stringbank, 0), patricia_count(stringbank));
  ptree *files = toc("files");
  printf("files bank: %p\n", files);
  printf("files depth:  %lld , count:  %lld \n", patricia_depth(files, 0),
         patricia_count(files));
  printf("TOC depth: %d\n", btree_depth(p->table_of_contents, 0));
  printf("patricia depth:  %lld \n", patricia_depth(toc("patricia"), 0));
  printf("patricia count:  %lld \n", patricia_count(toc("patricia")));

  /*printf("maps depth: %d\n", patricia_depth(toc("tags"), 0));
  printf("maps count: %d\n", patricia_count(toc("tags")));
  printf("points depth: %d\n", patricia_depth(toc("points"), 0));
  printf("points count: %d\n", patricia_count(toc("points")));
  */

  // printf("patricia count: %d\n", patricia_count(toc("patricia"), 0));
  printf("Pool allocated at %p\n", p);
  printf("Pool free space starts at %p\n", p->start_free);
  printf("Used %d%% ( %lld  of  %lld )\n", 100 * p->start_free / p->size,
         p->start_free, p->size);
}

// Start megapool.  Calling any other mega_ functions before this will
// segfault(hopefully)
mega_pool *start_megapool(char *filename, int size) {
  hidden_pool = mega_create_or_resize_pool(filename, size);
  mega_pool *p = hidden_pool;

  ptree *stringbank = toc("stringbank");

  if (!stringbank) {
    // Register it
    ptree *newStringbank = patricia_insert(p, stringbank, p->stringbank_word,
                                           11, 0, p->stringbank_word, 11);

    printf("New String bank: %p\n", newStringbank);

    // FIXME add --balance option
    // newStringbank = btree_balance(newStringbank);
    //  Save the stringbank
    p->table_of_contents = btree_set(p, p->table_of_contents,
                                     p->stringbank_word, 11, newStringbank, 6);
  }

  return p;
}

// I would like to invite every member of the C standards committee to pucker
// up, bend over and kiss my fucking arse
void *make_point(float lon, float lat) {
  void *a, *b;
  memcpy(&a, &lon, 8);
  memcpy(&b, &lat, 8);
  return ll_cons(a, b);
}
