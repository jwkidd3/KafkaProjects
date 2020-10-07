package guru.learningjournal.kafka.examples;

class AppConfigs {

    final static String applicationID = "CountingSessionApp";
    final static String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
    final static String topicName = "user-clicks";
    final static String stateStoreName = "tmp/state-store";
}
