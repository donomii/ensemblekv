## BoltDB, SQLite, and NuDB

SQLite uses `kv.sqlite` with the `kv_store` table and WAL journaling. BoltDB uses `bolt.db` and the `Blocks` bucket; transaction errors are propagated. NuDB, Pudge, and S3 retain their existing storage formats.

- `Get` returns the current value or an error. `Put` replaces the current value when the selected backend supports replacement. NuDB is insert-only and returns its duplicate-key error for an attempted overwrite.
- Prefix matching compares the literal bytes of the prefix. Case, SQL wildcard characters, zero bytes, and non-UTF-8 bytes have no special meaning.
- Iteration permits callback writes. It does not promise a transactional snapshot across concurrent modifications. Returned BoltDB iteration values and JsonKV values do not reference mutable database storage.

BoltDB rejects empty keys.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
