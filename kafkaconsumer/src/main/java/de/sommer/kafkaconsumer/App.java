package de.sommer.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import spark.Spark;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) {
        // Kafka consumer configuration settings
        String topicName = "topic_name";  //TODO: Replace with topic name
        Properties props = new Properties();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //TODO: Replace with port and IP
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test"); //TODO: Replace all after this with correct values
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        // Start a web server on port 4567
        Spark.port(4567);

        Spark.get("/consume", (req, res) -> {
            StringBuilder result = new StringBuilder();
            result.append("<h1>Messages</h1>");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                result.append("offset = ").append(record.offset())
                      .append(", key = ").append(record.key())
                      .append(", value = ").append(record.value())
                      .append("<br>");
            }
            return result.toString();
        });

        Spark.awaitInitialization();
    }
}
