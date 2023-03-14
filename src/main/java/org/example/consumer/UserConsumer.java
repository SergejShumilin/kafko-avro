package org.example.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.util.List;
import java.util.Properties;

public class UserConsumer {

    private final Consumer<String, GenericRecord> consumer;
    private final String topic;
    public UserConsumer(String topic, String kafkaServer, String schemaRegistry) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        properties.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put("schema.registry.url", schemaRegistry);

        this.consumer=new KafkaConsumer<String, GenericRecord>(properties);
    }

    public void consume() {
        consumer.subscribe(List.of(topic));
        try {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(5000);

                for (ConsumerRecord record : records){
                    System.out.println(record);
                }
        } finally {
        consumer.close();
        }
    }
}
