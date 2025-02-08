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
    private final String BOT_TOKEN = System.getenv("BOT_TOKEN");
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
        OptionData topicOption = new OptionData(OptionType.STRING, "topic", "Topic des Endpunktes", true)
            .addChoice("Home Assistant", "home_assistant_1")
            .addChoice("Discord Bot", "discordBot");
        api.updateCommands().addCommands(
            Commands.slash("consumer", "Kafka Konsument hinzufügen")
            .addOptions(topicOption),
            Commands.slash("unsubscribe", "Kafka Konsument entfernen")
            .addOptions(topicOption)
        ).queue();
    }

    public Kafka getKafka() {
        return kafka;
    }

}
