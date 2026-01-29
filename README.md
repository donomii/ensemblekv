# ensemblekv and extentkv
Scales to vast amounts of data by distributing data across separate key-value stores.

## Motivation and Design

I wanted a key-value store that could hold 10 Tb of data, and I couldn't find anything that could come close.  The best projects seemed to top out at around 300Gb.  There were a lot of projects with great README files showing off their great design, but when I put them to the test, half didn't even work.  Of the rest, none were really functional above ~300Gb, with some failing completely and others just slowing down to the point they were unusable.

I didn't want to write my own key-value store, so instead I wrote a small program to open 100 key-value stores, and wrote the data evenly across all of them.

But even that didn't work too well, so I gave up and wrote my own key-value store 

# EnsembleKV: Scalable Multi-Store Key-Value Systems

EnsembleKV is a collection of key-value store implementations designed for handling large-scale data with efficient distribution. It provides multiple approaches to distributing data across underlying stores, each optimized for different use cases.

## Core Features

- Automatic data distribution
- Support for multiple backend stores
- Configurable block sizes and store limits
- Simple, consistent interface across all implementations

## Available Stores

EnsembleKV is a store manager.  It creates and manages multiple stores, and distributes data across them.  This gives excellent scalability, and good parallelism.

To use EnsembleKV, you need to create a store manager, and also hand it a creator function that will create the underlying stores.  The underlying stores can be any of the stores listed below.  

You can also use these base stores directly, if you don't need the scalability.

### EnsembleKV

The ensemble store uses consistent hashing to distribute data across multiple substores. 

```go
// Create a new EnsembleKV store using BoltDB as the backend
store, err := EnsembleCreator("/path/to/store", 4096, BoltDbCreator)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use the store
err = store.Put([]byte("key"), []byte("value"))
value, err := store.Get([]byte("key"))
```

Key features:
- Initial creation with N substores (default 3)
- Even distribution using consistent hashing
- Excellent for random access patterns
- Highly parallel

## Available Substores

The ensemble stores can use any of these backend stores:

- **BoltDB**: Reliable B+tree-based store with ACID guarantees
- **ExtentKV**: Simple append-only store with good write performance.  Kind to spinning disks.
- **JsonKV**: Simple JSON-based store for testing and small datasets
- **MegaPoolKV**: Tree-based append-only store with good crash resistance.  High overhead.
- **SQLiteKV**:  SQLite-based store, key-value only (no SQL)
- **NuDB**
- **PudgeDB**

SQLite and BoltDB are the well known projects, just with a thin compatibility layer.  Both work well, but can't handle large datasets, so they work very well with EnsembleKV.

## Common Interface

All stores, including the ensemble and backend stores, implement the `KvLike` interface:

```go
type KvLike interface {
    Get(key []byte) ([]byte, error)
    Put(key []byte, value []byte) error
    Exists(key []byte) bool
    Delete(key []byte) error
    Size() int64
    Flush() error
    Close() error
    MapFunc(f func([]byte, []byte) error) (map[string]bool, error)
    Keys() [][]byte
}
```

## Configuration

Each store type accepts these common parameters:

- `directory`: Base directory for the store
- `blockSize`: Size of storage blocks (typically 10*1024*1024)
- `createStore`: Function to create backend stores
- `fileSize`: Size of the file to create, or max size of the file, depending on the store type.

Additional store-specific parameters:

### EnsembleKV
```go
type EnsembleKv struct {
    N           int    // Number of initial substores (can't be changed after creation)
    maxKeys     int    // Maximum keys per substore
    maxBlock    int    // Maximum block size
}
```



## Example: Choosing the Right Store

1. For general purpose use with good scaling:
```go
store := EnsembleCreator("/path/to/store", 10*1024*1024, ExtentCreator, 10*1024*1024)
```



## Error Handling

All operations return errors that should be checked:

```go
if err := store.Put(key, value); err != nil {
    log.Printf("Failed to store key: %v", err)
    // Handle error
}

value, err := store.Get(key)
if err != nil {
    if strings.Contains(err.Error(), "key not found") {
        // Handle missing key
    } else {
        // Handle other errors
    }
}
```

The data stores panic on serious errors, to prevent corruption.  You should install recovery blocks around database operations.

## Contributing

Contributions are welcome!

## License

AGPL-3.0
```