package com.example.kafkatool;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMsgConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMsgConsumer.class);

    private static final String KAFKA_SERVER = "localhost:9092";

    private static final String SOURCE_TOPIC = "test-topic-1";

    /**
     * Consumes all messages from {@link #SOURCE_TOPIC} on {@link #KAFKA_SERVER}
     */
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-message-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // read source topic from the beginning
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(SOURCE_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> oneRecord : records) {
                String flowId = null;
                for (Header header : oneRecord.headers()) {
                    if (header.key().equals("flowId")) {
                        flowId = new String(header.value());
                    }
                }
                log.info("Consuming msg flowId={} with key={} and value={}", flowId, oneRecord.key(), oneRecord.value());
            }
        }
    }
}
