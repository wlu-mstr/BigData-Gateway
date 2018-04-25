# BigData-Gateway
A big data enabled system including data ingest, query and analysis.
Users could define business data schema, table schema and then ingest data.
Data analysis ability is also enabled, including realtime data aggregation and batch analysis.

Several big data components are used:
- Kafka
- Spark Streaming
- Phoenix and HBase
- Hive

## application-register
A http service for registering application and specific event types.
An application need to register itself before ingesting/querying data to/from big data gateway.
- register with an account;
- an application key is generated and return to application;
- register event type with an account and application key.
Application key + event type is unique, and they must be post to server together with the message body.

## dataschema-config
A http service for configurations including input data schema (json based), json field to table columns mapping, Phoenix table DDLs etc..
Http post
- application key
- event type
- // table name is {application key}_{event type}
- json field data type, and its mapping to table column
Application key + event type is the key of configuration information for schema lookup.

## kafka-gateway
A http service for data ingest to gateway. Applications post data to http server and data are pushed to Kafka. There is only one Kafka topic. 
Http post:
- application key
- event type
- event body (json)
Application key + event type is the Kafka message key.

## dataingest-spark
An **internal** spark streaming job
- pulls message keys and values from Kafka;
- paser each message body by schema configuration, which is specific for each kafka key (application key + event type);
- ingest parsed message body to Phoenix table

## dataquery-gateway
A http service for data query. Only data ingested to the system are avalible for queying. Sql based.
- application key
- event type
- query sql

## dataanalysis-gateway
A http service for data analysis based on Hive
- aggregation of ingested data
- enrichment of ingested data with data from other systems, such as RDBMs, HDFS etc..
