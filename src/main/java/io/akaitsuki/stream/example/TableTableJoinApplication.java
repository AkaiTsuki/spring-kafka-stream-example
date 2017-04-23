package io.akaitsuki.stream.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;

import java.util.Properties;

/**
 * Created by jiachiliu on 4/22/17.
 */
public class TableTableJoinApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TableTableJoinApplication.class);
    private static final String APP_NAME = "Table-Table-Join-Example-App-v1";

    private static final String USER_CDC_TOPIC = "test-user-cdc-v1";
    private static final String COMPANY_CDC_TOPIC = "test-company-cdc-v1";
    private static final String USER_EVENT_TOPIC = "test-user-event-v1";
    private static final String USER_STATE_STORE = "test-user-state-v1";
    private static final String COMPANY_STATE_STORE = "test-company-state-v1";
    private static final String USER_EVENT_STATE = "test-user-event-state-v1";
    private static final String USER_OUT_TOPIC = "test-user-out-v1";

    public static void main(String[] args) {
        SpringApplication.run(TableTableJoinApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        log.info("Running application {}", APP_NAME);

        Serde<String> stringSerde = Serdes.String();
        KStreamBuilder builder = new KStreamBuilder();

        KTable<String, String> companyTable = builder.table(stringSerde, stringSerde, COMPANY_CDC_TOPIC, COMPANY_STATE_STORE);

        KTable<String, String> userTable = builder.table(stringSerde, stringSerde, USER_CDC_TOPIC, USER_STATE_STORE);

        KTable<String, String> userEventTable = builder.table(stringSerde, stringSerde, USER_EVENT_TOPIC, USER_EVENT_STATE);

        userEventTable
                .leftJoin(userTable, (e, u) -> e + ":" + u)
                .leftJoin(companyTable, (eu, c) -> eu + ":" + c)
                .print("Event-User-Company-Topic");

        StreamsConfig config = new StreamsConfig(props());
        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private Properties props() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "30000");

        return props;
    }


}
