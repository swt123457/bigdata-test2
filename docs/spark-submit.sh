## --jars hdfs:///home/admin/crm/spark/spark-sql-kafka-0-10_2.11-2.2.2.jar

jar_deps=hdfs:///home/admin/crm/spark/spark-sql-kafka-0-10_2.11-2.2.2.jar
jar_file=hdfs:///home/admin/crm/spark/sparkTest-1.0-SNAPSHOT.jar

hiveTable=dwd.fact_crm_customer_risk_score_final_incr
kafkaTopic=crm-customer-risk-score
fields=customer_id,merge_type,operation_score

spark-submit --master yarn \
--deploy-mode cluster \
--total-executor-cores 8 \
--driver-memory 2g \
--executor-memory 6g \
--jars ${jar_deps} \
--name push_hive_kafka \
--class com.swt.test.spk.PushHiveToKafka ${jar_file} ${hiveTable} ${kafkaTopic} ${fields}




spark-submit --master yarn --deploy-mode cluster --total-executor-cores 6 --driver-memory 2g --executor-memory 6g --jars hdfs:///home/spark/spark-sql-kafka-0-10_2.11-2.2.2.jar --name push_hive_kafka_test --class com.swt.test.spk.PushHiveToKafka hdfs:///home/spark/sparkTest-1.0-SNAPSHOT.jar  dwd.fact_crm_customer_risk_score_final_incr crm-customer-risk-score


spark-submit --master yarn --deploy-mode cluster --total-executor-cores 8 --driver-memory 2g --executor-memory 6g --jars hdfs:///home/admin/crm/spark/spark-sql-kafka-0-10_2.11-2.2.2.jar --name push_hive_kafka --class com.swt.test.spk.PushHiveToKafka hdfs:///home/admin/crm/spark/sparkTest-1.0-SNAPSHOT.jar dwd.fact_crm_customer_risk_score_final_incr crm-customer-risk-score   customer_id,merge_type,operation_score




/opt/apache/flink-1.11.1/bin/flink run -m yarn-cluster -ytm 1024 -ynm flink-sync-kafka-ck -c com.swt.test.flink.SyncKfkToCK flinkTest-1.0-SNAPSHOT.jar bigdata_app-mysql_stream_test test.mysql_stream_test task_rule_id,assign_batch_no,month,assign_mode,assign_by,assign_by_id,status,rowkey



flink run -m yarn-cluster -ytm 1024 -ynm flink-test -c com.test.FlinkTest hdfs:///home/flink_test.jar
