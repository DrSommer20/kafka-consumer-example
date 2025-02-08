package de.sommer.kafkaconsumer.connection;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Kafka {
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private Set<String> subscribedTopics = new HashSet<>();

    public Kafka() {
        createKafkaConsumer();
        createKafkaProducer();
        subscribeToTopic("discordBot");
        subscribeToTopic("home_assistant_1");
    }

    private void createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.121:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DiscordBot");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
    }

    private void createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.121:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void subscribeToTopic(String topic) {
        subscribedTopics.add(topic);
        consumer.subscribe(subscribedTopics);
    }

    public void sendMessageToKafka(String topic, String message) {
        producer.send(new ProducerRecord<>(topic, message));
    }
}