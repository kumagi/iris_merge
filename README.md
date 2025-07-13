# Iris Merge

Iris Merge is a new storage engine that takes Substrait protobuf as input and returns aggregated or joined tables.
Internally, Iris Merge supports journaling, which enables users to get rapid responses to write requests.
Iris Merge uses a storage format similar to a *Log-Structured Row Table*. A periodic internal merge operation consolidates updated and inserted rows into a single large Parquet table.
It maintains multiple Parquet tables and supports JOIN operations across them.

