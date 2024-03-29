# rust-dataflow-lab

Trying out the Timely Dataflow library.

## Timely Dataflow Cheat Sheet

Dataflow operators are operations on streams of data. 
You can read about the dataflow operators here: https://timelydataflow.github.io/timely-dataflow/chapter_2/chapter_2_3.html

Some notes:

`exchange` is used to distribute data to workers according to a hash function. It is useful for sharding to ensure that
all data with the same key goes to the same worker. Also known as "physical" partitioning.

`inspect` is used to look at data in the stream.

`map` works as expected, it is used to transform data in the stream, producing a new stream. A mutable variant is also
available.

`accumulate` collects data for an epoch and works like a reduce function. If data is streamed with the epochs set
accordingly, it can be used to implement e.g. a windowed aggregation like sums, counts, min, max, etc.
