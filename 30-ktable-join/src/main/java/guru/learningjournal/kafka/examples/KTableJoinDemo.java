package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.UserDetails;
import guru.learningjournal.kafka.examples.types.UserLogin;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class KTableJoinDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, UserDetails> KT0 = streamsBuilder.table(AppConfigs.userMasterTopic,
            Consumed.with(AppSerdes.String(), AppSerdes.UserDetails())
        );

        KTable<String, UserLogin> KT1 = streamsBuilder.table(AppConfigs.lastLoginTopic,
            Consumed.with(AppSerdes.String(), AppSerdes.UserLogin())
        );

        KT0.join(KT1, (v1,v2) ->{
            v1.setLastLogin(v2.getCreatedTime());
            return v1;
        }).toStream().print(Printed.toSysOut());

       

        logger.info("Starting Stream...");
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams...");
            streams.close();
        }));

    }
}
