package guru.learningjournal.kafka.examples;

class AppConfigs {

    public final static String applicationID = "KTableJoinDemo";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String userMasterTopic = "user-master";
    public final static String lastLoginTopic = "user-login";
    public final static String stateStoreName = "tmp/state-store";
}
