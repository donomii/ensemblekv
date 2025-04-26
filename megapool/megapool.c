#define MAGIC_NUMBER 0x4D504B56  // "MPKV" (MegaPool KV)

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
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

// Keep the binary tree implementation
#include "binary_tree_plus.c"
#include "linked_list.c"

void *address_base = (void *)0x1000000000;
int verbose = 0;

void v(const char *fmt, ...) {
  if (verbose) {
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
  }
}

// Open an existing megapool file
mega_pool *mega_open_pool(char *filename) {
  mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
      filename, ii_get_file_length(filename), (ptr)address_base);
  if (pool == NULL) {
    printf("Failed to open pool file %s\n", filename);
    abort();
  }
  if (pool->magic != MAGIC_NUMBER) {
    printf("Error: File %s is not a valid megapool (expected magic number 0x%08X, found 0x%08X).\n", filename, MAGIC_NUMBER, pool->magic);
    abort();
  }
  return pool;
}

// Allocate new memory inside the pool
ptr mega_malloc(mega_pool *pool, ii_int size) {
  if (size < 1) {
    abort();
  }
  kv_info(pool);
  char *p = (char *)pool;
  char *new_alloc = p + pool->start_free;
  pool->start_free = pool->start_free + size;
  if (pool->size <= pool->start_free) {
    printf("Out of memory while trying to allocate megapool memory! Extend "
           "the megapool!\n");
    exit(1);
  }
  return (void *)new_alloc;
}

// Insert data into the pool
ptr mega_insert(mega_pool *p, const void *data, ii_int len) {
  ptr e = mega_malloc(p, len);
  memcpy(e, data, len);
  return e;
}

// Setup a new pool file
mega_pool *mega_new_pool(char *filename, ii_int size) {
    printf("Initialising file %s\n", filename);
    mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
        filename, size, (ptr)address_base);
    if (pool == NULL) {
      printf("Unable to initialise %s to size %llu\n", filename, size);
    }
    pool->magic = MAGIC_NUMBER;
    pool->start_free = sizeof(mega_pool);
    pool->size = size;
    pool->recommended_address = pool;
    pool->btree_root = 0;
    pool->btree_root = btree_insert(pool,0, NULL, 0, NULL, 0);
    if (!btree_validate(pool, pool->btree_root)) {
      printf("Tree validation failed after creating new pool\n");
      abort();
    }
    kv_info(pool);
    return pool;
  }

// Open the file on disk and resize as needed
mega_pool *mega_create_or_resize_pool(char *filename, ii_int size) {
  printf("Char * is %d bytes long\n", sizeof(char *));
  printf("Void * is %d bytes long\n", sizeof(void *));
  printf("ii_int is %d bytes long\n", sizeof(ii_int));
  ii_int flen = ii_get_file_length(filename);
  printf("Existing file length: %d\n", flen);
  if (flen == 0) {
    if (size < 1) {
      size = 100000000;
      printf("File does not exist, chose default file size 100000000\n");
    }
    return mega_new_pool(filename, size);
  } else {
    if (size == -1) {
      size = flen;
    }
    printf("Using size %d\n", flen);
    // resize
    if (size != flen) {
      printf("Resizing and mapping from %d to %d\n", flen, size);
      mega_pool *pool = (mega_pool *)ii_size_and_map_file_to_address(
          filename, size, (ptr)address_base);
      pool->size = size;

      return pool;
    } else {
      return mega_open_pool(filename);
    }
  }
}

// Current pool size (on disk and in memory)
int mega_current_size(mega_pool *pool) { return pool->size; }

// Confirm pointer is inside pool
void check_in_pool(mega_pool *pool, void const *data, char *msg) {
  if (data == NULL) {
    return;
  }
  if (data < (void *)pool) {
    printf("Pointer %p is lower than megapool (%p): %s\n", data, pool, msg);
    btree *p = NULL;
    printf("%s", p->data);
    exit(1);
    abort();
  }
  if (data > ((void *)pool + pool->size)) {
    printf("Pointer %p is outside megapool (%p - %p): %s\n", data, pool,
           (void *)pool + pool->size, msg);
    btree *p = NULL;
    printf("%s", p->data);
    exit(1);
    abort();
  }
}

// Add a key-value pair to the btree
void kv_put(mega_pool *p, const char *key, const char *value) {
    // First, check if the key already exists
    ii_int existing = 0;
    if (p->btree_root) {
      existing = btree_search(p, p->btree_root, key, strlen(key) + 1);
    }
    
    // Allocate memory in the pool for the key and value
    void *key_copy = mega_insert(p, key, strlen(key) + 1);
    void *value_copy = mega_insert(p, value, strlen(value) + 1);
    
    if (existing) {
      // If key exists, update its value
      ii_int new_root = btree_set(p, p->btree_root, key_copy, strlen(key) + 1,
                             value_copy, strlen(value) + 1);
      p->btree_root = new_root;
    } else {
      // If key doesn't exist, insert new node
      ii_int new_root = btree_insert(p, p->btree_root, key_copy, strlen(key) + 1,
                                value_copy, strlen(value) + 1);
      p->btree_root = new_root;
    }
  }
  
const char *kv_get(mega_pool *p, const char *key) {
    if (!p->btree_root) {
      return NULL;
    }
    
    ii_int node_offset = btree_search(p, p->btree_root, key, strlen(key) + 1);
    
    if (node_offset) {
      btree *node = btree_from_offset(p, node_offset);
      return (const char *)node->data;
    }
    
    return NULL;
  }

// Delete a key-value pair
void kv_delete(mega_pool *p, const char *key) {
  ii_int root = p->btree_root;
  ii_int node = btree_search(p, root, key, strlen(key) + 1);
  if (node) {
    ii_int new_root = btree_delete_node(p, root, node);
    p->btree_root = new_root;
  }
}



// Show info about the store
void kv_info(mega_pool *p) {
  printf("Key-Value Store Statistics:\n");
  printf("  Total size: %lld bytes\n", p->size);
  printf("  Used: %lld bytes (%.2f%%)\n", p->start_free,
         (float)p->start_free * 100.0 / (float)p->size);
  printf("  Free: %lld bytes\n", p->size - p->start_free);
  printf("  Tree root offset: %d\n", btree_depth(p, p->btree_root, 0));
  printf("  Node count: %d\n", btree_count(p, p->btree_root));
}

// Callback for printing entries
int print_entry(const void *key, int key_len, void *data, int data_len, void *userdata) {
    printf("Key: %p, Key Length: %p, Data: %p, Data Length: %p\n",key, key_len, data, data_len);
    if (key == NULL || data == NULL) {
      return 0;  // Skip null entries
    }
    if (key_len <= 0 || data_len <= 0) {
      printf("Invalid key or data length\n");
      return 0;  // Error
    }
  printf("Key: %s, Value: %s\n", (char*)key, (char*)data);
  return 0;
}

// Start megapool
mega_pool *start_megapool(char *filename, int size) {
  mega_pool *pool = mega_create_or_resize_pool(filename, size);
  return pool;
}