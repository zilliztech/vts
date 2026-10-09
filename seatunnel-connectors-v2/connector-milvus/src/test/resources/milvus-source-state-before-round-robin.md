# Legacy Milvus enumerator state fixture

`milvus-source-state-before-round-robin.ser` was written by `DefaultSerializer`
using the unmodified `MilvusSourceState` from VTS commit `5c7a2c4fb9`.
The old class was compiled with Lombok 1.18.38 and Java target 8. Its implicit
`serialVersionUID`, obtained from `ObjectStreamClass.lookup`, is
`1718378968826165653`.

The state contains an `ArrayList` with pending table `default.remaining_collection`
and a `HashMap` mapping reader 2 to an `ArrayList` with one split:

- Table: `default.source_collection`
- Collection: `source_collection`
- Partition: `partition_a`
- Split ID: `source_collection-offset-250-limit-250`
- Offset and limit: 250

The serialized class has no `nextAssignment` field. Keep this fixture generated
by the old class: serializing the current class would not test upgrade compatibility.
