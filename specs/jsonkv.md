## JsonKV

`store.json` is an array of objects containing `key` and `value` byte strings, encoded as base64 by JSON. Entries are saved in byte-key order. This preserves arbitrary binary keys without UTF-8 replacement or collisions. Duplicate keys in the array and a null store are rejected.

The previous JSON object format, with text keys and base64 values, remains readable. Subsequent saves use the array format; older library versions cannot read this new format. Saving synchronizes a temporary file, replaces `store.json` by rename, and synchronizes the containing directory. Existing file permissions are preserved; a new store file is private to its owner.

`Flush` completes the backend persistence operation and reports errors. `Close` releases storage resources. Errors must not be interpreted as successful persistence.

Regression tests use real local backing stores and filesystem operations. Remote services are not required.
