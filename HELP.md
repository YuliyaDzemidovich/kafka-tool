# Kafka tool in Java

### Use cases:

* Manually send 1 message to specified kafka topic.
* Resend all messages from one kafka topic to another kafka topic

### How to:
#### Preparation steps for test:
1. Start local kafka from ./docker folder:
```
doker-compose up -d
```
2. Open Kafdrop (Kafka UI) at http://localhost:9001/ and create one or several kafka topics, for example `test-topic-1` and `test-topic-2`.
#### Use options:
1. You can send message manually to any topic by running main method in `KafkaOneMsgProducer.java`
2. You can resend messages from one kafka topic to another by running main method in `KafkaMsgResender.java`