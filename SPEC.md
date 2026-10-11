# EnsembleKV storage behavior

EnsembleKV is a Go library for storing byte-string keys and values in local databases and distributing them among backing stores. The command in `tool/` opens a selected store to set a value or inspect a key's available history. The library has no graphical interface.

## Public operations

`KvLike` provides `Get`, `Put`, `Exists`, `Delete`, `Size`, `Flush`, `Close`, `Keys`, `MapFunc`, `MapPrefixFunc`, `DumpIndex`, and `KeyHistory`. A creator receives a directory, block size, and file-size setting and returns a store or an error. Managers receive another creator for their backing stores.

- `Get` returns the current value or an error. `Put` replaces the current value when the selected backend supports replacement. NuDB is insert-only and returns its duplicate-key error for an attempted overwrite.
- `Delete` makes a key absent. Treatment of already absent keys varies by backend. Empty keys are rejected by BoltDB, Extent, and SingleFileLSM. SingleFileLSM uses a nil value as a deletion marker; a non-nil empty value remains a value.
- `Keys` returns current live keys. Manager bookkeeping prefixes and deletion records are not user keys.
- Iteration calls the callback with keys and values and returns a map describing visited keys. A false map value denotes a deletion record when exposed by that implementation. Callback errors stop iteration and are returned to the caller.
- Prefix matching compares the literal bytes of the prefix. Case, SQL wildcard characters, zero bytes, and non-UTF-8 bytes have no special meaning.
- Iteration permits callback writes. It does not promise a transactional snapshot across concurrent modifications. Returned BoltDB iteration values and JsonKV values do not reference mutable database storage.
- `Flush` completes the selected backend's persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.
- `Size` is currently backend-dependent: some implementations return live-key counts, whereas SingleFileLSM and MegaPool return file sizes. Managers inherit this limitation when applying size thresholds.
- History is backend-dependent and is not a general versioned-read guarantee. Some adapters implement only the latest value or no history. NuDB, Pudge, and S3 do not implement `Keys` and cannot be used in operations requiring that method.

## Ensemble manager

An Ensemble store has a fixed number of substores. A key hash modulo that count determines its substore. The directory contains `metadata.json` with generation, substore count, and total-key bookkeeping, and a generation directory containing numbered substore directories. The count is restored from metadata when reopening.

Operations on one substore are serialized. Explicit flushing visits the substores, and a background worker periodically flushes stores marked dirty using the same substore locks. Close waits for compaction, stops the worker, and closes the active substores. Requests check the lifecycle state after acquiring their substore lock.

Compaction processes substores sequentially. Each attempt creates a fresh sibling `.compacting` directory, snapshots the source's live keys while registering the replacement, and copies keys under the substore lock. Successful reads of empty values remain empty values. Writes during copying update both stores; deletions are recorded so an uncopied key cannot reappear. A source or replacement mutation failure invalidates the attempt. Enumeration, read, flush, close, filesystem, and reopen errors are returned rather than interpreted as missing keys or successful persistence.

Publication holds the substore lock. Both stores are flushed and closed, and their files and directories are synchronized. A sibling `.compaction` file containing `ensemble-compaction-v1` followed by a newline is synchronized before the first rename. The original directory becomes `.old`, the replacement becomes the primary, and the replacement is reopened and its live-key set checked against the final source state. Renaming `.old` to `.retired` and synchronizing the containing directory commits the replacement before requests resume. Cleanup removes `.retired`, synchronizes that removal, then removes the version marker. A failed rollback or uncertain commit synchronization makes the substore unavailable until it is closed and reopened; operations must not acknowledge writes against an uncertain recovery state.

At startup, a valid version marker makes `.old` authoritative for an unfinished swap. Without a marker, `.old` can be restored when no primary exists; simultaneous primary and backup from an older library are ambiguous and are preserved with an error. Empty unused directories before initial metadata creation are cleaned up as bootstrap artifacts. `.retired` means the primary was committed; the primary must exist and validate before the backup is removed. Recovery checks the existing local storage layout before invoking a creator, so missing files cannot silently become a new empty database. Conflicting backup states, invalid markers, a lone `.compacting` directory, or invalid authoritative data cause an error. A stale `.compacting` directory beside an authoritative source is discarded. Recovery relies on the filesystem honoring file and directory synchronization; the suite exercises persisted directory states and actual local I/O errors, not power-loss behavior of storage hardware.

Compaction uses checked enumeration for supported local backends and recursively for Ensemble, LineLSM, TreeLSM, and StarLSM. Incomplete TreeLSM and StarLSM child sets are rejected. Custom backing stores must provide `KeysWithError() ([][]byte, error)` with complete live keys or an error; interrupted recovery additionally requires a recognized local storage layout. Backends without reliable enumeration are rejected. This does not change the existing `KvLike` interface. Compaction retains key sets in memory; SingleFile checksum validation also reads whole sorted tables. Temporary disk space must accommodate a full replacement. It preserves current values and deletions, not historical values. Each completed substore remains compacted if a later substore fails.

## LineLSM

LineLSM uses four ordered tiers. Lower tiers are newer. Backing keys have `d:` prefixes for data and `t:` prefixes for deletion markers; user keys may themselves contain either prefix. Deletion markers have a non-empty value so backends interpreting nil as deletion retain them.

Lookup examines tiers from newest to oldest and stops at the first value or deletion marker. A newer value is not hidden by an older deletion. Merging a value removes an older destination deletion marker. Public enumeration resolves this same ordering and returns logical keys exactly once.

The directory contains `metadata.json` and backing-store directories named `tier-<level>-store-<identifier>`. Metadata records `tiers`, `max_tier_sizes`, and `last_flush`; each tier contains `level`, `store_ids`, and `total_size`. Store IDs are directory basenames. Reopening uses the listed IDs and rejects missing or invalid referenced stores. Existing store directories with absent or empty tier metadata are ambiguous and are rejected rather than silently opened as an empty database.

Metadata replacement writes and synchronizes a temporary file before renaming it and synchronizing the directory. Flush merges through the tiers and flushes the surviving backing stores.

## TreeLSM and StarLSM

These managers partition a store into sixteen child stores using successive hexadecimal hash characters. Once a split completes, children own lookup, mutation, and enumeration. Old parent copies do not make deleted keys reappear. A missing-key lookup never recurses back into its parent.

TreeLSM child directories use `substore_` followed by the hexadecimal encoding of the complete ASCII hash prefix. Loading decodes that prefix. StarLSM child directory names are single lowercase hexadecimal characters; the full prefix and depth are reconstructed from ancestry. Both managers reopen nested children and enumerate leaf data.

The current format does not record an atomic split-publication state. Interruption during child creation or redistribution is outside the completed-split recovery guarantee.

## Extent

Each store uses `keys.dat`, `values.dat`, `keys.index`, and `values.index`. Both indexes contain signed big-endian 64-bit boundaries beginning at zero. Their corresponding records must have the same count. A negative starting boundary marks a deletion; the magnitude is the byte position. Ending-boundary signs do not change the preceding record's value. Repeated value boundaries represent empty values.

Reopening validates the initial boundaries and finds the common complete record prefix. Trailing partial index entries, unpaired records, or entries extending beyond available data are discarded together with their incomplete data suffixes. Recovery never extends a data file to invent missing bytes. Invalid or missing initial indexes for an existing store are rejected.

Index caching changes lookup cost, not accepted operations or results. Deleted and negatively cached missing keys are excluded from enumeration. Cache replacement and access are synchronized with mutations. Iteration releases storage locks before calling user code and propagates callback failures. Flush reports errors from every data and index synchronization.

## SingleFileLSM

The file begins with a 4096-byte version-1 header, followed by a reserved write-ahead-log region and appended sorted tables and table indexes. The header records WAL start, table-region boundary, current index position, table count, and checksum. WAL entries encode checksum, key length, value length, flags, key bytes, and value bytes; a value length of minus one denotes deletion. Keys contain 1 through 1 GiB of bytes and values may contain up to 1 TiB, subject to available resources.

Each append fits entirely inside its WAL reservation. If necessary, pending entries are flushed, and a larger reservation is appended without overwriting existing tables. A durable end marker and payload are written before an entry header makes the entry visible to recovery. Recovery retains the append position of recovered records.

Flushing appends a new table and replacement index after existing data, synchronizes and publishes the replacement header, then invalidates the old WAL start. Older table indexes remain intact until replacement metadata is published. Enumeration resolves table entries newest first, with the current memory table taking precedence. Compaction additionally checks the published table count, table checksums, and individual table records and rejects incomplete or corrupt enumeration.

Existing version-1 files remain readable. These guarantees do not repair data already overwritten by an earlier implementation or provide recovery from a torn single-copy file header.

## JsonKV

`store.json` is an array of objects containing `key` and `value` byte strings, encoded as base64 by JSON. Entries are saved in byte-key order. This preserves arbitrary binary keys without UTF-8 replacement or collisions. Duplicate keys in the array and a null store are rejected.

The previous JSON object format, with text keys and base64 values, remains readable. Subsequent saves use the array format; older library versions cannot read this new format. Saving synchronizes a temporary file, replaces `store.json` by rename, and synchronizes the containing directory. Existing file permissions are preserved; a new store file is private to its owner.

## Other backends and validation

MmapSingleKV stores append-only records in `data.log`. Flush synchronously writes the entire used mapped region and synchronizes the file. SQLite uses `kv.sqlite` with the `kv_store` table and WAL journaling. BoltDB uses `bolt.db` and the `Blocks` bucket; transaction errors are propagated. MegaPool, NuDB, Pudge, and S3 retain their existing storage formats.

The regression suite uses real local backing stores and filesystem operations. It covers persistence, interrupted writes, key enumeration, deletion and reinsertion, binary prefixes and keys, callback mutation, storage errors, and race detection. Remote services are not required.
