package io.akaitsuki.stream.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by jiachiliu on 10/7/17.
 */
public class StreamTableLeftJoinTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private StreamTableLeftJoin app;

    @BeforeClass
    public static void startCluster() throws InterruptedException, IOException {
        CLUSTER.start();
        CLUSTER.createTopic(StreamTableLeftJoin.USER_INFO_TOPIC);
        CLUSTER.createTopic(StreamTableLeftJoin.USER_CLICK_TOPIC);
        CLUSTER.createTopic(StreamTableLeftJoin.OUTPUT_TOPIC);
    }

    @AfterClass
    public static void stopCluster() {
        CLUSTER.stop();
    }

    @Before
    public void init() {
        Properties overwrite = new Properties();
        overwrite.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamTableLeftJoinTest");
        overwrite.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        overwrite.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        System.out.println(overwrite);

        app = new StreamTableLeftJoin(overwrite);
        app.start(true);
    }

    @After
    public void teardown() {
        app.close();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Properties consumer1Config = getConsumerProperties("StreamTableLeftJoinTest-consumer-1");

        // Produce user click event's first without user information
        List<KeyValue<String, String>> clickEvents = new ArrayList<>();
        clickEvents.add(new KeyValue<>("1", "c1"));
        clickEvents.add(new KeyValue<>("1", "c2"));
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(StreamTableLeftJoin.USER_CLICK_TOPIC, clickEvents, producerConfig, new Date().getTime());

        List<KeyValue<String, String>> actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumer1Config, StreamTableLeftJoin.OUTPUT_TOPIC, 2);
        List<KeyValue<String, String>> expect = Arrays.asList(
                new KeyValue<>("1", "null c1"),
                new KeyValue<>("1", "null c2")
        );
        assertThat(actual).containsExactlyElementsOf(expect);

        // Produce user information
        List<KeyValue<String, String>> userEvents = Arrays.asList(
                new KeyValue<>("1", "John Smith"),
                new KeyValue<>("2", "Linda Winsfield")
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(StreamTableLeftJoin.USER_INFO_TOPIC, userEvents, producerConfig, new Date().getTime());

        // Produce user click events again
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(StreamTableLeftJoin.USER_CLICK_TOPIC, clickEvents, producerConfig, new Date().getTime());
        actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumer1Config, StreamTableLeftJoin.OUTPUT_TOPIC, 2);
        expect = Arrays.asList(
                new KeyValue<>("1", "John Smith c1"),
                new KeyValue<>("1", "John Smith c2")
        );
        assertThat(actual).containsExactlyElementsOf(expect);

        // Produce another user click events
        clickEvents = new ArrayList<>();
        clickEvents.add(new KeyValue<>("2", "c1"));
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(StreamTableLeftJoin.USER_CLICK_TOPIC, clickEvents, producerConfig, new Date().getTime());
        actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumer1Config, StreamTableLeftJoin.OUTPUT_TOPIC, 1);
        expect = Collections.singletonList(
                new KeyValue<>("2", "Linda Winsfield c1")
        );
        assertThat(actual).containsExactlyElementsOf(expect);
    }

    private Properties getConsumerProperties(String groupId) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }
}