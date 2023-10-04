# producer.py
import time
import pyarrow as pa

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]


batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])

# Send the Arrow Array through IPC
sink = "/tmp/sink"
time.sleep(2)
with pa.ipc.new_stream(sink, batch.schema) as writer:
   writer.write_batch(batch)

print(f"{time.time():0.5f}: Producer: wrote\n{batch}")
