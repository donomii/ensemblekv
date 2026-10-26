## TreeLSM and StarLSM

These managers partition a store into sixteen child stores using successive hexadecimal hash characters. Once a split completes, children own lookup, mutation, and enumeration. Old parent copies do not make deleted keys reappear. A missing-key lookup never recurses back into its parent.

TreeLSM child directories use `substore_` followed by the hexadecimal encoding of the complete ASCII hash prefix. Loading decodes that prefix. StarLSM child directory names are single lowercase hexadecimal characters; the full prefix and depth are reconstructed from ancestry. Both managers reopen nested children and enumerate leaf data.

The current format does not record an atomic split-publication state. Interruption during child creation or redistribution is outside the completed-split recovery guarantee.

- `Keys` returns current live keys. Manager bookkeeping prefixes and deletion records are not user keys.
- Iteration calls the callback with keys and values and returns a map describing visited keys. A false map value denotes a deletion record when exposed by that implementation. Callback errors stop iteration and are returned to the caller.

Iteration permits callback writes. It does not promise a transactional snapshot across concurrent modifications.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
