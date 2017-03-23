Hive UDF, UDAF, Serde Example
========

### Description
UDF, UDAF, Serde Example

### Serde Table Create
CREATE EXTERNAL TABLE CHAT(`datetime` string,`schema` string,`version` int,`message` string) PARTITIONED BY (key STRING) ROW FORMAT SERDE 'example.hive.serde.SchemaLogSerde' WITH SERDEPROPERTIES ("input.schema.name"="CHAT") LOCATION '/logs/CHAT';

CREATE EXTERNAL TABLE BATTLE(`datetime` string,`gameid` string,`playtime` int) PARTITIONED BY (key STRING) ROW FORMAT SERDE 'example.hive.serde.MultiLineLogSerde' WITH SERDEPROPERTIES ("input.schema.name"="BATTLE") STORED AS INPUTFORMAT 'example.hive.serde.multiline.MultiLineLogContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/logs/RESULT_LOG.TXT' TBLPROPERTIES ("input.schema.name"="BATTLE");
