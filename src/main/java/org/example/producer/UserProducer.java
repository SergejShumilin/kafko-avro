package org.example.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class UserProducer<T> {
    private final KafkaProducer<String, T> kafkaProducer;
    private final String topic;

    public UserProducer(String topic, String kafkaServer, String schemaRegistry) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        properties.put("schema.registry.url", schemaRegistry);
        kafkaProducer = new KafkaProducer(properties);
    }

    public void produce(T t){
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(topic, "key", t);

        try {
            kafkaProducer.send(producerRecord);
        } catch (SerializationException e) {
            e.printStackTrace();
        }
    }

    public void shutdown(){
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
