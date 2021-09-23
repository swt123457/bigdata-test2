package com.swt.common.utils;

import com.swt.common.entity.KafkaBinlogEntity;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaBinlogSchema implements KafkaDeserializationSchema<KafkaBinlogEntity> {
    @Override
    public boolean isEndOfStream(KafkaBinlogEntity nextElement) {
        return false;
    }

    @Override
    public KafkaBinlogEntity deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        KafkaBinlogEntity msg = JsonUtil.json2Bean(new String(record.value(), StandardCharsets.UTF_8), KafkaBinlogEntity.class);
        return msg;
    }

    @Override
    public TypeInformation<KafkaBinlogEntity> getProducedType() {
        return TypeInformation.of(new TypeHint<KafkaBinlogEntity>() {});
    }
}
