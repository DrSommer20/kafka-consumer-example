package de.sommer.kafkaconsumer.discordBot;

import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.entities.User;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MessageListener extends ListenerAdapter {
    private static final String COMMAND_CHANNEL = "kafka-befehle";
    private static final String PRODUCER_CHANNEL = "kafka-produzenten";

    private Map<String, Set<User>> consumerSubscriptions = new HashMap<>();
    private Map<String, Set<User>> producerSubscriptions = new HashMap<>();

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        Message message = event.getMessage();
        String content = message.getContentRaw();
        System.out.println(content);

        if (event.getChannel().getName().equals(PRODUCER_CHANNEL)) {
            String topic = producerSubscriptions.entrySet().stream()
                    .filter(e -> e.getValue().contains(event.getAuthor()))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            notifyConsumers(topic, content);
        }
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        if (!event.getChannel().getName().equals(COMMAND_CHANNEL)) {
            event.reply("Commands are only allowed in the " + COMMAND_CHANNEL + " channel.").setEphemeral(true).queue();
            return;
        }

        if (event.getName().equals("kafka")) {
            event.deferReply().queue();
            String type = event.getOption("type").getAsString();
            String topic = event.getOption("topic").getAsString();
            User user = event.getUser();

            if (type.equals("consumer")) {
                consumerSubscriptions.computeIfAbsent(topic, k -> new HashSet<>()).add(user);
                event.getHook().sendMessage("User " + user + " has subscribed to topic " + topic).queue();
            } else if (type.equals("producer")) {
                producerSubscriptions.computeIfAbsent(topic, k -> new HashSet<>()).add(user);
                System.out.println("User " + user + " added to topic:  " + topic);
                event.getHook().sendMessage("User " + user + " registered as producer for topic:" + topic).queue();
            }
        }

        if (event.getName().equals("unsubscribe")) {
            event.deferReply().queue();
            String topic = event.getOption("topic").getAsString();
            User user = event.getUser();

            if (consumerSubscriptions.containsKey(topic)) {
                if(consumerSubscriptions.get(topic).remove(user))
                    event.getHook().sendMessage(user + " has unsubscribed as a consumer from topic " + topic).queue();
            }

            if (producerSubscriptions.containsKey(topic)) {
                if(producerSubscriptions.get(topic).remove(user))
                    event.getHook().sendMessage(user + " has unsubscribed as a producer from topic " + topic).queue();
            }
        }
    }

    private void notifyConsumers(String topic, String message) {
        if (consumerSubscriptions.containsKey(topic)) {
            for (User user : consumerSubscriptions.get(topic)) {
                user.openPrivateChannel().queue(channel -> channel.sendMessage("New message in topic " + topic + ": " + message).queue());
            }
        }
    }
}


