package ru.company.nano;

public interface IBroker {
    void subscribe(ISubscriber subscriber, String topic);

    void unsubscribe(ISubscriber subscriber, String topic);

    boolean send(Message msg);
}
