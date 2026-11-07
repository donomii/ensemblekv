## LineLSM

LineLSM uses four ordered tiers. Lower tiers are newer. Backing keys have `d:` prefixes for data and `t:` prefixes for deletion markers; user keys may themselves contain either prefix. Deletion markers have a non-empty value so backends interpreting nil as deletion retain them.

Lookup examines tiers from newest to oldest and stops at the first value or deletion marker. A newer value is not hidden by an older deletion. Merging a value removes an older destination deletion marker. Public enumeration resolves this same ordering and returns logical keys exactly once.

SingleFileLSM backing stores enumerate table entries newest first, with the current memory table taking precedence. LineLSM depends on this order when removing an older tombstone.

The directory contains `metadata.json` and backing-store directories named `tier-<level>-store-<identifier>`. Metadata records `tiers`, `max_tier_sizes`, and `last_flush`; each tier contains `level`, `store_ids`, and `total_size`. Store IDs are directory basenames. Reopening uses the listed IDs and rejects missing or invalid referenced stores. Existing store directories with absent or empty tier metadata are ambiguous and are rejected rather than silently opened as an empty database.

Metadata replacement writes and synchronizes a temporary file before renaming it and synchronizing the directory. Flush merges through the tiers and flushes the surviving backing stores.

- `Keys` returns current live keys. Manager bookkeeping prefixes and deletion records are not user keys.
- Iteration calls the callback with keys and values and returns a map describing visited keys. A false map value denotes a deletion record when exposed by that implementation. Callback errors stop iteration and are returned to the caller.

Iteration permits callback writes. It does not promise a transactional snapshot across concurrent modifications.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
