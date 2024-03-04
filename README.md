# rust-dataflow-lab

Trying out the Timely Dataflow library

## Timely Dataflow Cheat Sheet

Some notes:
 

`exchange` is used to distribute data to workers according to a hash function. It is useful for sharding to ensure that
all data with the same key goes to the same worker.

`inspect` is used to look at data in the stream.

`map` works as expected, it is used to transform data in the stream, producing a new stream. A mutable variant is also
available.


