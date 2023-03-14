package org.example;

import org.example.consumer.UserConsumer;
import org.example.producer.UserProducer;

public class Main {
    public static void main(String[] args) {
        String topic = "user-topic";
        String schemaRegistry = "http://localhost:8081";
        String kafkaServer = "localhost:9092";

        UserProducer userProducer = new UserProducer(topic, kafkaServer, schemaRegistry);
        UserConsumer userConsumer = new UserConsumer(topic, kafkaServer, schemaRegistry);

        AnotherUser anotherUser = AnotherUser.newBuilder().setMessage("Some message from another user").build();
        User user = User.newBuilder().setMessage("Some message from user").build();
        try {
            userProducer.produce(user);
            userProducer.produce(anotherUser);
            userConsumer.consume();
        } catch (Throwable throwable){
            throwable.printStackTrace();
        } finally {
            userProducer.shutdown();
        }
    }
}
