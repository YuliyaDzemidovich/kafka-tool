package com.example.kafkatool;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaOneMsgProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOneMsgProducer.class);

    private static final String KAFKA_SERVER = "localhost:9092";

    private static final String DESTINATION_TOPIC = "test-topic-1";

    /**
     * Sends one message with msgKey and msgValue to the {@link #DESTINATION_TOPIC} to {@link #KAFKA_SERVER}
     */
    public static void main(String[] args) {
        String flowId = UUID.randomUUID().toString();
        log.info("Publishing message with flowId={}", flowId);

        String msgKey = "key3";
        String msgValue = "msg3";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(producerProps);

        ProducerRecord<String, String> msg = new ProducerRecord<>(DESTINATION_TOPIC, msgKey, msgValue);
        msg.headers().add("flowId", flowId.getBytes(StandardCharsets.UTF_8));

        producer.send(msg);
        producer.flush();
    }
}
