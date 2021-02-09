# Daily Ingestion of SFTP Data Source

## Background
Data source from data provider services containing stock indexes. An index is comprised of a curated list of stocks and 
their accompanying metrics. These metrics vary across providers. There are typically two sets of data for each trading 
day with a ‘snapshot’ of the index made at the closing of the market and an adjusted close index which is used the 
following morning. Typically, such data contains information regarding the company, identification codes for varying 
platforms and various other metrics relating to the company.

The service is a rolling window subscription-based service with each trading day having approximately 57 files with 
varying upload times. There is a backlog of 7 days on the server with the oldest files removed from access as new ones 
are added.

Within the 57 files that are released each day there are different file types and contents with each index having files 
with values on closes and adjusted bases, with other file types relating to corporate actions. The format of the data is 
typically comma-separated values with differing file extensions.

The data of interest is the S&P ASX 300 constituent files on a closed basis. Accuracy of the data from the provider is 
to be retained.

## Requirements
- Retain archives for flexible and independent use of data.
- Any changes regarding the contents of the files made by the source whilst they are on the on the server should to be 
flagged and accounted for, using potential change indicators such as the modified time and the size of the file.
- Parse and archive data from a sftp server and upload to a database. 

## Design

This design is up to known descriptions and I will try to elaborate on possible scenarios in practice. First I can notice
that data between each file has different set of fields and also file types. It's not clear what can be assumed with 
certainty regarding data schemas from our service providers. For now, I have implemented a function to scan available 
data to define a generic flat table schema containing all possible fields across all files. The table will be store in 
a SQLite local database on disk for demonstration purpose. See `magellan.Pipeline.init_table` method for details.

All files appear to be tab separated file format, however
some file has an end line with the line count as summary, i.e. the last line in some files isn't part of the intended 
file content. This can possibly be used to detect data integrity but it depends on the service providers consistently 
setting this summary at the end of file. This line summary is ignore by the demo implementation for now.

Although not demonstrated here, I think it's best to mirror this SFTP data source to an S3 bucket for archival, with 
object life cycle setting so that files older than e.g. 30 days are moved to glacier tier for cost saving. This can be 
setup using a periodic lambda to simply read SFTP data into S3 with boto3 library. Having S3 in the architecture also 
help with versioning, e.g. S3 will retain the latest version of any file. We can also enable S3 versioning to keep older
file versions too but it depends on the business requirements. 

Another benefit of having S3 is that we can register to notification of any new S3 put for new files (or versions) 
ingested and perform necessary ETL using a separate lambda. So we will have an S3 lambda and another ETL lambda each 
handling a separate concern. While parsing data, we come across records with e.g. `(DATE_OF_INDEX, INDEX_CODE)` already 
exist in the SQL table. Since this table is indexed by `(DATE_OF_INDEX, INDEX_CODE)`, we need to consolidate these new
records with data in the table. However, given that the logic for this consolidation isn't available at the moment, I 
have simply let the new records merged with the existing SQL data for same primary keys and replace the old record.

Regarding the S3 lambda, it's best to delete the file from SFTP once it is successfully stored in S3. This simplify 
implementation and save cost thanks to shorter lambda runtime subsequently. However if deleting SFTP files isn't an 
option, this lambda need to maintain a persistent list of `[{filename, timestamp, checksum}]` for files processed in the window. 
Such list can be kept in the same S3 bucket as e.g. a json file.

A final benefit of S3 is that storing csv files there enable us to query data directly with SQL using Athena, which is
serverless and can be cheaper than maintaining an RDS instance for the same purpose. This is because S3 storage is much
cheaper than RDS storage.

## Installation
```shell script
virtualenv venv -p python3
. venv/bin/activate
pip install requirement.txt
python3
```
## Ingestion Demo
```python
from magellan import Pipeline
from sink import Sink

pipeline = Pipeline()
pipeline.init_table()
pipeline.load()

sink = Sink()
sink.fetch(10)
```