package de.sommer.kafkaconsumer.discordBot;

import de.sommer.kafkaconsumer.connection.Kafka;
import io.github.cdimascio.dotenv.Dotenv;
import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.OptionData;
import net.dv8tion.jda.api.requests.GatewayIntent;

public class KafkaBot {
    private JDA api;
    private Dotenv dotenv = Dotenv.load();
    private final String BOT_TOKEN = dotenv.get("BOT_TOKEN");
    private Kafka kafka;

    public KafkaBot() throws Exception {
        kafka = new Kafka();
        api = JDABuilder.createDefault(BOT_TOKEN)
          .enableIntents(GatewayIntent.MESSAGE_CONTENT)
          .addEventListeners(new MessageListener(this))
          .build();
        addKafkaCommand();
        
    }


    private void addKafkaCommand(){
        api.updateCommands().addCommands(Commands.slash("kafka", "Kafka Endpunkt erstellen")
            .addOptions(
                new OptionData(OptionType.STRING, "type", "Typ des Endpunktes", true)
                    .addChoice("Konsument", "consumer")
                    .addChoice("Produzent", "producer"))
            .addOption(OptionType.STRING, "topic", "Topic des Endpunktes", true),
            Commands.slash("unsubscribe", "Kafka Endpunkt entfernen")
            .addOption(OptionType.STRING, "topic", "Topic des Endpunktes", true)
        ).queue();
    }

    public Kafka getKafka() {
        return kafka;
    }

}
