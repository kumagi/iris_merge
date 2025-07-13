# Iris Merge

Iris Merge is a new storage engine which takes Substrait protobuf as input and returns aggregated or joined input table.
Internally, Iris Merge supports journaling which enables users to get rapid respond to write requests.
On the other hand, Iris Merge stores storage like *Log Structured Row Table*. The periodical internal merge operation makes each updated or inserted row into a single big parquet table.
It maintains multiple parquet table and supports JOIN operation across them.

