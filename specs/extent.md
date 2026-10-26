## Extent

Each store uses `keys.dat`, `values.dat`, `keys.index`, and `values.index`. Both indexes contain signed big-endian 64-bit boundaries beginning at zero. Their corresponding records must have the same count. A negative starting boundary marks a deletion; the magnitude is the byte position. Ending-boundary signs do not change the preceding record's value. Repeated value boundaries represent empty values.

Reopening validates the initial boundaries and finds the common complete record prefix. Trailing partial index entries, unpaired records, or entries extending beyond available data are discarded together with their incomplete data suffixes. Recovery never extends a data file to invent missing bytes. Invalid or missing initial indexes for an existing store are rejected.

Index caching changes lookup cost, not accepted operations or results. Deleted and negatively cached missing keys are excluded from enumeration. Cache replacement and access are synchronized with mutations. Iteration releases storage locks before calling user code and propagates callback failures. Flush reports errors from every data and index synchronization.

Empty keys are rejected. `Keys` returns current live keys.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
