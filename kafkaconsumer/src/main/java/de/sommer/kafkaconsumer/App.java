package de.sommer.kafkaconsumer;

import de.sommer.kafkaconsumer.discordBot.KafkaBot;

public class App 
{
    public static void main(String[] args) {
        try {
            new KafkaBot();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}

