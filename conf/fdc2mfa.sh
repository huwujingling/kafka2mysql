#bin/bash
/opt/cloudera/parcels/CDH/lib/flume-ng/bin/flume-ng \
agent -c /opt/oudera/parcels/CDH/lib/flume-ng/conf \
-f /etldata/ETL_SCRIPT/FLUME_SCRIPT/config/fdc_2_mfa.properties \
-n agent -Dflume.root.logger=INFO,console -Xmx2048m \
--classpath "/etldata/ETL_SCRIPT/JAR/Kafka2MysqlIntercepter.jar:/etldata/ETL_SCRIPT/JAR/MysqlSink.jar:" \
-Dflume.monitoring.type=http -Dflume.monitoring.port=30002