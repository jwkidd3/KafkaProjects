package guru.learningjournal.kafka.examples.serde;

import guru.learningjournal.kafka.examples.types.AdClick;
import guru.learningjournal.kafka.examples.types.AdImpression;
import guru.learningjournal.kafka.examples.types.CampaignPerformance;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {

    static final class AdImpressionSerde extends WrapperSerde<AdImpression> {
        AdImpressionSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdImpression> AdImpression() {
        AdImpressionSerde serde = new AdImpressionSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdImpression.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class AdClickSerde extends WrapperSerde<AdClick> {
        AdClickSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<AdClick> AdClick() {
        AdClickSerde serde = new AdClickSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, AdClick.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class CampaignPerformanceSerde extends WrapperSerde<CampaignPerformance> {
        CampaignPerformanceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<CampaignPerformance> CampaignPerformance() {
        CampaignPerformanceSerde serde = new CampaignPerformanceSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, CampaignPerformance.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
