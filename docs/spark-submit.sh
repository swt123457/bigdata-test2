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
