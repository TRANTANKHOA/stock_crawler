# Installation
```shell script
virtualenv venv -p python3
. venv/bin/activate
pip install requirement.txt
python3
```
# Ingestion
```python
from magellan import Pipeline
from sink import Sink

pipeline = Pipeline()
pipeline.init_table()
pipeline.load()

sink = Sink()
sink.fetch(10)
```