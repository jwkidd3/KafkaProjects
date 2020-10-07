package guru.learningjournal.kafka.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.DepartmentAggregate;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

import spark.Spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AppRestService {

    private static final Logger Logger = LogManager.getLogger();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaStreams streams;
    private final HostInfo hostInfo;
    private Client client;
    private Boolean isActive = false;
    private final String NOT_FOUND = "{\"code\":204, \"message\":\"No Content\"}";
    private final String SERVICE_UNAVAILABLE = "{\"code\":503, \"message\":\"Service Unavailable\"}";

    AppRestService(KafkaStreams stream,String hostname,int port){
        this.streams = stream;
        this.hostInfo = new HostInfo(hostname,port);
        client = ClientBuilder.newClient();



    }

    private String getValue(String searchKey) throws JsonProcessingException {
        ReadOnlyKeyValueStore<String,DepartmentAggregate> departmentStore = streams.store(
                AppConfigs.stateStoreName, QueryableStoreTypes.keyValueStore());

        DepartmentAggregate result = departmentStore.get(searchKey);
        return(result == null) ? NOT_FOUND : objectMapper.writeValueAsString(result);
    }

    private List<KeyValue<String, DepartmentAggregate>> getAllValues(){
        List<KeyValue<String,DepartmentAggregate>> results = new ArrayList<>();
        ReadOnlyKeyValueStore<String,DepartmentAggregate> departmentStore = streams.store(
                AppConfigs.stateStoreName, QueryableStoreTypes.keyValueStore());
        departmentStore.all().forEachRemaining(results::add);
        return results;
    }

    private String getValueFromRemote(String searchkey, HostInfo hostInfo){
        String result;
        String targetHost = String.format(("http://%s:%d/kv/%s"), hostInfo.host(),hostInfo.port(),searchkey);

        result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);
        return result;
    }

    private List<KeyValue<String,DepartmentAggregate>> getAllValuesFromRemote(HostInfo hostInfo) throws IOException {
        List<KeyValue<String,DepartmentAggregate>> results = new ArrayList<>();
        String targetHost = String.format(("http://%s:%d/dept/local"), hostInfo.host(),hostInfo.port());
        String result = client.target(targetHost).request(MediaType.APPLICATION_JSON).get(String.class);
        if(!result.equals(NOT_FOUND)){
            results = objectMapper.readValue(result,results.getClass());
        }
        return results;
    }

    void start(){
        Logger.info("Starting KTableAggDemo ...." + "http://" + hostInfo.host() + ":" + hostInfo.port());
        Spark.port(hostInfo.port());
        Spark.get("/getAggregate/:deptName",(req,res) -> {
            String results;
            String searchKey = req.params(":deptName");
            Logger.info("Requests/getAggregate/" + searchKey);
            if(!isActive){
                results= SERVICE_UNAVAILABLE;
            }else{
                StreamsMetadata metadata = streams.metadataForKey(AppConfigs.stateStoreName,searchKey, AppSerdes.String().serializer());
                if(metadata.hostInfo().equals(hostInfo)){
                    Logger.info("Retrieving key/value from local ......");
                    results = getValue(searchKey)
                }else{
                    Logger.info("Retrieving key/value from Remote ....");
                    results = getValueFromRemote(searchKey,metadata.hostInfo());
                }
            }
            return results;
        });

        Spark.get("/getAggregate/all",(req,res) -> {
            List<KeyValue<String,DepartmentAggregate>> allResults = new ArrayList<>();
            String results;
            if(!isActive){
                results = SERVICE_UNAVAILABLE;
            }else{
                Collection<StreamsMetadata> AllMetaData = streams.allMetadataForStore(AppConfigs.stateStoreName);
                for(StreamsMetadata metadata:AllMetaData){
                    if(metadata.hostInfo().equals(hostInfo)){
                        Logger.info("Retrieving all the key/value pairs from Local ....");
                        allResults.addAll(getAllValues());
                    }else{
                        Logger.info("Retrieving all key/value pairs from Remote ....");
                        allResults.addAll(getAllValuesFromRemote(metadata.hostInfo()));

                    }
                }
                results = (allResults.size() == 0) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return results;
        });

        Spark.get("/getAggregate/local",(req,res) -> {
            List<KeyValue<String,DepartmentAggregate>> allResults = new ArrayList<>();
            String results;
            if(!isActive){
                results = SERVICE_UNAVAILABLE;
            }else{
                Logger.info("Retrieving all key/value pairs from Local...");
                allResults = getAllValues();
                results = (allResults.size() == 0 ) ? NOT_FOUND : objectMapper.writeValueAsString(allResults);
            }
            return results;

        });
    }

    void setActive(Boolean state){isActive = state;}

    void stop(){
        client.close();
        Spark.stop();
    }

}
