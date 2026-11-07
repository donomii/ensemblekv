## SingleFileLSM

The file begins with a 4096-byte version-1 header, followed by a reserved write-ahead-log region and appended sorted tables and table indexes. The header records WAL start, table-region boundary, current index position, table count, and checksum. WAL entries encode checksum, key length, value length, flags, key bytes, and value bytes; a value length of minus one denotes deletion. Keys contain 1 through 1 GiB of bytes and values may contain up to 1 TiB, subject to available resources.

Each append fits entirely inside its WAL reservation. If necessary, pending entries are flushed, and a larger reservation is appended without overwriting existing tables. A durable end marker and payload are written before an entry header makes the entry visible to recovery. Recovery retains the append position of recovered records.

Flushing appends a new table and replacement index after existing data, synchronizes and publishes the replacement header, then invalidates the old WAL start. Older table indexes remain intact until replacement metadata is published. Enumeration resolves table entries newest first, with the current memory table taking precedence.

Existing version-1 files remain readable. These guarantees do not repair data already overwritten by an earlier implementation or provide recovery from a torn single-copy file header.

Empty keys are rejected. `Keys` returns current live keys.

SingleFileLSM uses a nil value as a deletion marker; a non-nil empty value remains a value.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
