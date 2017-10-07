package io.akaitsuki.stream.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Created by jiachiliu on 10/6/17.
 *
 * A Stream Table left join example. Stream is user click event, table is user name
 */
public class StreamTableLeftJoin extends AbstractTopology {

    public final static String APPLICATION_ID = "StreamTableLeftJoinExample";

    /**
     * Topic contains user click message where event key is user id and value is the object user click on
     */
    public final static String USER_CLICK_TOPIC="user-click";

    /**
     * topic contains user information such as name
     */
    public final static String USER_INFO_TOPIC="user-info";

    /**
     * output topic with enhanced user click message where key is user id and value is user's name and object the user
     * click on
     */
    public final static String OUTPUT_TOPIC="user-click-output";

    public StreamTableLeftJoin() {
        super();
    }

    public StreamTableLeftJoin(Properties overwrite) {
        super(overwrite);
    }


    @Override
    protected void construct(KStreamBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        KTable<String,String> userTable = builder.table(USER_INFO_TOPIC);
        builder.stream(stringSerde, stringSerde, USER_CLICK_TOPIC).leftJoin(userTable, (click, user) -> user + " " + click).to(OUTPUT_TOPIC);
    }

    @Override
    protected Properties props() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return props;
    }
}
