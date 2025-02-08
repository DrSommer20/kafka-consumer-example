package de.sommer.kafkaconsumer.discordBot;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.entities.User;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;

public class MessageListener extends ListenerAdapter {
    private static final String COMMAND_CHANNEL = "kafka-befehle";
    private static final String PRODUCER_CHANNEL = "kafka-produzenten";
    private static final Set<String> AVAILABLE_TOPICS = Set.of("home_assistant_1", "discordBot");

    private Map<String, Set<User>> consumerSubscriptions = new HashMap<>();
    private KafkaBot kafkaBot;

    public MessageListener(KafkaBot kafkaBot) {
        this.kafkaBot = kafkaBot;
        startKafkaConsumer();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        Message message = event.getMessage();
        String content = message.getContentRaw();
        System.out.println(content);

        if (event.getChannel().getName().equals(PRODUCER_CHANNEL)) {
            kafkaBot.getKafka().sendMessageToKafka("discordBot", content);
        }
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        if (!event.getChannel().getName().equals(COMMAND_CHANNEL)) {
            event.reply("Commands are only allowed in the " + COMMAND_CHANNEL + " channel.").setEphemeral(true).queue();
            return;
        }

        if (event.getName().equals("consumer")) {
            event.deferReply().queue();
            String topic = event.getOption("topic").getAsString();
            User user = event.getUser();

            if (!AVAILABLE_TOPICS.contains(topic)) {
                event.getHook().sendMessage("Invalid topic. Please choose either 'home_assistant_1' or 'discordBot'.").queue();
                return;
            }
            consumerSubscriptions.computeIfAbsent(topic, k -> new HashSet<>()).add(user);
            event.getHook().sendMessage("User " + user.getEffectiveName() + " has subscribed to topic " + topic).queue();

        }

        if (event.getName().equals("unsubscribe")) {
            event.deferReply().queue();
            String topic = event.getOption("topic").getAsString();
            User user = event.getUser();

            if (consumerSubscriptions.containsKey(topic)) {
                if (consumerSubscriptions.get(topic).remove(user))
                    event.getHook().sendMessage(user.getEffectiveName() + " has unsubscribed as a consumer from topic " + topic).queue();
            }
        }
    }

    private void startKafkaConsumer() {
        new Thread(() -> {
            KafkaConsumer<String, String> consumer = kafkaBot.getKafka().getConsumer();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    notifyConsumers(record);
                });
            }
        }).start();
    }

    private void notifyConsumers(ConsumerRecord<String, String> record) {
        if (consumerSubscriptions.containsKey(record.topic())) {
            for (User user : consumerSubscriptions.get(record.topic())) {
                user.openPrivateChannel().queue(channel -> {
                    String formattedMessage = formatMessage(record);
                    channel.sendMessage(formattedMessage).queue();
                });
            }
        }
    }
        
    private String formatMessage(ConsumerRecord<String, String> record) {
        StringBuilder sb = new StringBuilder();
        sb.append("**New message in topic** `").append(record.topic()).append("`:\n");
        sb.append("```json\n");
        sb.append("{\n");
        sb.append("  \"Offset\": ").append(record.offset()).append(",\n");
        sb.append("  \"Key\": \"").append(record.key()).append("\",\n");
        sb.append("  \"Value\": ").append(record.value()).append(",\n");
        sb.append("  \"Partition\": ").append(record.partition()).append(",\n");
        sb.append("  \"Timestamp\": ").append(record.timestamp()).append(",\n");
        sb.append("  }\n");
        sb.append("}\n");
        sb.append("```\n");
        return sb.toString();
    }
}