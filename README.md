# ensemblekv and extentkv
Scales to vast amounts of data by distributing data across separate key-value stores.

## Motivation and Design

I wanted a key-value store that could hold 10 Tb of data, and I couldn't find anything that could come close.  The best projects seemed to top out at around 300Gb.  There were a lot of projects with great README files showing off their great design, but when I put them to the test, half didn't even work.  Of the rest, none were really functional above ~300Gb, with some failing completely and others just slowing down to the point they were unusable.

I didn't want to write my own key-value store, so instead I wrote a small program to open 100 key-value stores, and wrote the data evenly across all of them.


# EnsembleKV: Scalable Multi-Store Key-Value Systems

EnsembleKV is a collection of key-value store implementations designed for handling large-scale data with efficient distribution and automatic scaling. It provides multiple approaches to distributing data across underlying stores, each optimized for different use cases.

## Core Features

- Automatic data distribution and rebalancing
- Support for multiple backend stores
- Concurrent access with thread safety
- Automatic store splitting when size thresholds are reached
- Configurable block sizes and store limits
- Simple, consistent interface across all implementations

## Available Stores

### EnsembleKV

The base ensemble store that uses consistent hashing to distribute data across multiple substores. When substores reach capacity, it automatically doubles the number of substores and redistributes the data.

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
- Automatic doubling of substores when capacity is reached
- Even distribution using consistent hashing
- Good for random access patterns

### TreeLSM

A hierarchical store that organizes data in a tree structure based on key hashes. Each node splits into 16 substores when capacity is reached.

```go
// Create a new TreeLSM store using JsonKV as the backend
store, err := NewTreeLSM("/path/to/store", 4096, JsonKVCreator)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use the store
err = store.Put([]byte("key"), []byte("value"))
value, err := store.Get([]byte("key"))
```

Key features:
- Hierarchical organization with prefix-based routing
- 16-way splits when capacity is reached
- Good for prefix-based queries
- Efficient for localized access patterns

### StarLSM

Similar to TreeLSM but uses a more dynamic splitting strategy based on hash prefixes. Splits create new levels of depth as needed.

```go
// Create a new StarLSM store using ExtentKV as the backend
store, err := NewStarLSM("/path/to/store", 4096, ExtentCreator)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use the store
err = store.Put([]byte("key"), []byte("value"))
value, err := store.Get([]byte("key"))
```

Key features:
- Dynamic depth-based splitting
- Hash-based distribution
- Good for uniform data distribution
- Efficient for random access patterns

### LineLSM

A tiered LSM-tree implementation that organizes data in levels with increasing size limits. Supports automatic compaction and merging.

```go
// Create a new LineLSM store using BoltDB as the backend
store, err := LineLSMCreator("/path/to/store", 4096, BoltDbCreator)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Use the store
err = store.Put([]byte("key"), []byte("value"))
value, err := store.Get([]byte("key"))
```

Key features:
- Multiple tiers with increasing size limits
- Automatic compaction and merging
- Support for tombstones
- Good for write-heavy workloads

## Available Backend Stores

The ensemble stores can use any of these backend stores:

- **BoltDB**: Reliable B+tree-based store with ACID guarantees
- **ExtentKV**: Simple append-only store with good write performance
- **JsonKV**: Simple JSON-based store for testing and small datasets

## Common Interface

All stores implement the `KvLike` interface:

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
}
```

## Configuration

Each store type accepts these common parameters:

- `directory`: Base directory for the store
- `blockSize`: Size of storage blocks (typically 4096)
- `createStore`: Function to create backend stores

Additional store-specific parameters:

### EnsembleKV
```go
type EnsembleKv struct {
    N           int    // Number of initial substores
    maxKeys     int    // Maximum keys per substore
    maxBlock    int    // Maximum block size
}
```

### LineLSM
```go
// Tier size limits
maxTierSizes: []int64{
    64 * 1024 * 1024,    // Tier 0: 64MB
    256 * 1024 * 1024,   // Tier 1: 256MB
    1024 * 1024 * 1024,  // Tier 2: 1GB
    4096 * 1024 * 1024,  // Tier 3: 4GB
}
```

### TreeLSM/StarLSM
```go
const maxStoreSize = 64 * 1024 * 1024 // 64MB before splitting
```

## Performance Considerations

- **EnsembleKV**: Best for random access patterns and uniform key distribution
- **TreeLSM**: Best for prefix-based queries and localized access patterns
- **StarLSM**: Best for uniform distribution with dynamic scaling
- **LineLSM**: Best for write-heavy workloads with periodic compaction

## Example: Choosing the Right Store

1. For general purpose use with good scaling:
```go
store := EnsembleCreator("/path/to/store", 4096, BoltDbCreator)
```

2. For prefix-based queries or hierarchical data:
```go
store := NewTreeLSM("/path/to/store", 4096, BoltDbCreator)
```

3. For write-heavy workloads:
```go
store := LineLSMCreator("/path/to/store", 4096, BoltDbCreator)
```

4. For dynamic scaling with uniform distribution:
```go
store := NewStarLSM("/path/to/store", 4096, BoltDbCreator)
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

## Contributing

Contributions are welcome! Areas that need attention:

- Performance benchmarking improvements
- Additional backend store implementations
- Better error handling and recovery
- Documentation and examples
- Test coverage

## License

AGPL-3.0
```