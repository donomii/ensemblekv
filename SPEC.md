# EnsembleKV storage behavior

EnsembleKV is a Go library for storing byte-string keys and values in local databases and distributing them among backing stores. The command in `tool/` opens a selected store to set a value or inspect a key's available history. The library has no graphical interface.

## Public operations

`KvLike` provides `Get`, `Put`, `Exists`, `Delete`, `Size`, `Flush`, `Close`, `Keys`, `MapFunc`, `MapPrefixFunc`, `DumpIndex`, and `KeyHistory`. A creator receives a directory, block size, and file-size setting and returns a store or an error. Managers receive another creator for their backing stores.

- `Size` is currently backend-dependent: some implementations return live-key counts, whereas SingleFileLSM and MegaPool return file sizes. Managers inherit this limitation when applying size thresholds.
- History is backend-dependent and is not a general versioned-read guarantee. Some adapters implement only the latest value or no history. NuDB, Pudge, and S3 do not implement `Keys` and cannot be used in operations requiring that method.

## Ensemble manager

An Ensemble store has a fixed number of substores. A key hash modulo that count determines its substore. The directory contains `metadata.json` with generation, substore count, and total-key bookkeeping, and a generation directory containing numbered substore directories. The count is restored from metadata when reopening.

Operations on one substore are serialized. Explicit flushing visits the substores, and a background worker periodically flushes stores marked dirty. Compaction copies each substore into a sibling `.compacting` directory, switches directories through a `.old` name, then removes the old copy. This specification does not assert crash-atomic or failure-atomic compaction; that path requires separate validation.

Backend and manager contracts are recorded in the Markdown files under `specs/`.
