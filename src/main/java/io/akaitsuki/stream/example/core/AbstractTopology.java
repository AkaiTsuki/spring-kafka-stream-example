package io.akaitsuki.stream.example.core;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/**
 * Created by jiachiliu on 10/7/17.
 */
public abstract class AbstractTopology implements Topology {

    protected static final String BOOTSTRAP_SERVERS="127.0.0.1:9092";

    protected KafkaStreams streams;

    public AbstractTopology() {
        KStreamBuilder builder = new KStreamBuilder();
        construct(builder);
        streams = new KafkaStreams(builder, props());
    }

    public AbstractTopology(Properties overwrite) {
        KStreamBuilder builder = new KStreamBuilder();
        construct(builder);
        Properties props = props();
        props.putAll(overwrite);
        streams = new KafkaStreams(builder, props);
    }

    /**
     * Construct topology by sub classes
     *
     * @param builder
     */
    protected abstract void construct(KStreamBuilder builder);

    protected abstract Properties props();

    @Override
    public void start(boolean cleanBeforeStart) {
        if(cleanBeforeStart) {
            streams.cleanUp();
        }
        streams.start();
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("Unexpected exception on thread " + t + ": " + e);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    @Override
    public void close() {
        streams.close();
    }
}
