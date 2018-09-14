package ru.company.nano;

public interface IBroker {
    void subscribe(ISubscriber subscriber, String topic);

    void unsubscribe(ISubscriber subscriber, String topic);

    void unsubscribeAll(ISubscriber subscriber);

    /**
     *
     * @param msg
     * @return true if the message was successfully was delivered to all recipients, false otherwise.
     */
    boolean send(Message msg);
}
