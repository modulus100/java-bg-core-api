package com.luminorgroup.poc.cdc.debezium;


import com.luminorgroup.poc.cdc.debezium.config.DebeziumProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaProducer<String, String> producer;

    public KafkaProducerService(@Autowired DebeziumProperties debeziumProperties) {
        this.producer = new KafkaProducer<>(getProducerProperties(debeziumProperties));
    }

    public void send(CDCMessage<String, String> message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(message.getTopic(), message.getKey(), message.getValue());
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // Optional: block until acknowledged
            log.info("KafkaProducerService:: Sent record to topic {} partition {} offset {}", metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            log.error("KafkaProducerService:: Failed to send record", e);
        }
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private static Properties getProducerProperties(DebeziumProperties debeziumProperties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, debeziumProperties.getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, debeziumProperties.getProducerConfig().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, debeziumProperties.getProducerConfig().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        return props;
    }

}
