package io.akaitsuki.stream.example;

/**
 * Created by jiachiliu on 10/6/17.
 */
public interface Topology {
    void start(boolean cleanBeforeStart);
    void close();
}
