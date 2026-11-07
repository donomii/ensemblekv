## MmapSingleKV

MmapSingleKV stores append-only records in `data.log`. Flush synchronously writes the entire used mapped region and synchronizes the file. MegaPool retains its existing storage format.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
