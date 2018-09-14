package ru.company.nano;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class Message {
    private final Collection<String> topics;
    private final Serializable data;

    public Message(String topic, Serializable data) {
        this(List.of(topic), data);
    }

    public Message(Collection<String> topics, Serializable data) {
        this.topics = Objects.requireNonNull(topics, "Topics can not be null.");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("Topics can not be empty");
        }
        this.data = Objects.requireNonNull(data, "Data can not be null.");
    }

    public Collection<String> getTopics() {
        return topics;
    }

    public Serializable getData() {
        return data;
    }
}
