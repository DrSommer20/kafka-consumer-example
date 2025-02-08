package de.sommer.kafkaconsumer.connection;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Kafka {
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private Set<String> subscribedTopics = new HashSet<>();

    public Kafka() {
        // subscribedTopics.add("testtopic");
        // createKafkaConsumer();
        // createKafkaProducer();
    }

    private void createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DiscordBot");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // receive messages that were sent before the consumer started
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create the consumer using props.
        try (final Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
            // subscribe to the topic.
            final String topic = "testtopic";
            consumer.subscribe(Arrays.asList(topic));
            // poll messages from the topic and print them to the console
            consumer
            .poll(Duration.ofMinutes(1))
            .forEach(System.out::println);
        }

    }

    private void createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("testtopic", Integer.toString(i), Integer.toString(i)));

        producer.close();

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

    public static void main(String[] args) {
        Kafka kafka = new Kafka();
        kafka.createKafkaProducer();
        kafka.createKafkaConsumer();
       

    }
}