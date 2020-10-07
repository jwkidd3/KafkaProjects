package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.HeartBeat;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;


public class WindowSuppressApp {
    private static final Logger Logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreName);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, HeartBeat> KS0 = streamsBuilder.stream(AppConfigs.topicName,
            Consumed.with(AppSerdes.String(), AppSerdes.HeartBeat())
                .withTimestampExtractor(new AppTimestampExtractor())
        );

        KTable<Windowed<String>,Long> KT01  =KS0.groupByKey(Grouped.with(AppSerdes.String(),AppSerdes.HeartBeat()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ofSeconds(10)))
        .count().suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        KT01.toStream().foreach(
                (wKey,value) -> Logger.info(
                        "App ID" + wKey.key() + "Window ID" + wKey.window().hashCode() +
                                "Window start:" + Instant.ofEpochMilli(wKey.window().start()).atOffset(ZoneOffset.UTC)+
                                "Window end" + Instant.ofEpochMilli(wKey.window().end()).atOffset(ZoneOffset.UTC) +
                                "Count" + value +
                                (value>2 ? "Application is Alive":"Application Failed- Sending Alert Email.....")
                )
        );


        Logger.info("Starting Stream...");
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.info("Stopping Streams...");
            streams.close();
        }));

    }
}
