# consumer.py
import time
import pyarrow as pa

sink = "/tmp/sink"
with pa.ipc.open_stream(sink) as reader:
      schema = reader.schema
      batches = [b for b in reader]
      print(f"{time.time():0.5f}: {schema}")
      print(f"{time.time():0.5f}: {batches}")


