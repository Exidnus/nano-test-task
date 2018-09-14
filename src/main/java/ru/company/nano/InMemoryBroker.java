package ru.company.nano;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class InMemoryBroker implements IBroker {
    private final ConcurrentMap<String, Set<ISubscriber>> topicToSubscribers = new ConcurrentHashMap<>();
    private final ISender sender;
    private final long timeoutMs;
    private final ExecutorService pool;

    public InMemoryBroker(ISender sender, long timeoutMs, int poolSize) {
        this.sender = Objects.requireNonNull(sender, "Sender can not be null.");
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Timeout can not be 0 or negative.");
        }
        this.timeoutMs = timeoutMs;
        this.pool = Executors.newFixedThreadPool(poolSize); //TODO add logging
    }

    @Override
    public void subscribe(ISubscriber subscriber, String topic) {
        check(subscriber, topic);

        Set<ISubscriber> oneSubscriber = ConcurrentHashMap.newKeySet();
        oneSubscriber.add(subscriber);

        topicToSubscribers.merge(topic, oneSubscriber, (oldSet, newSet) -> {
            Set<ISubscriber> allSet = ConcurrentHashMap.newKeySet(oldSet.size() + newSet.size());
            allSet.addAll(oldSet);
            allSet.addAll(newSet);
            return allSet;
        });
    }

    @Override
    public void unsubscribe(ISubscriber subscriber, String topic) {
        check(subscriber, topic);

        topicToSubscribers.computeIfPresent(topic, (key, subscribers) -> {
            Set<ISubscriber> copy = ConcurrentHashMap.newKeySet(subscribers.size());
            copy.addAll(subscribers);
            copy.remove(subscriber);
            return copy.isEmpty() ? null : copy;
        });
    }

    @Override
    public void unsubscribeAll(ISubscriber subscriber) {
        topicToSubscribers.values()
                .forEach(subscribers -> subscribers.remove(subscriber));
    }

    private void check(ISubscriber subscriber, String topic) {
        Objects.requireNonNull(subscriber, "Subscriber can not be null.");
        Objects.requireNonNull(topic, "Topic can not be null.");
    }

    @Override
    public boolean send(Message msg) {
        Objects.requireNonNull(msg, "Message can not be null.");
        List<CompletableFuture<Boolean>> allResults = getAllDeferredResults(msg);

        if (allResults.isEmpty()) {
            return false;
        }

        try {
            CompletableFuture.allOf(allResults.toArray(new CompletableFuture[0]))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            return false;
        } catch (InterruptedException ie) {
            throw new IllegalStateException("Thread was interrupted.", ie);
        } catch (ExecutionException ee) {
            throw new IllegalStateException("During sending exception occurred.", ee);
        }

        return checkResults(allResults);
    }

    private List<CompletableFuture<Boolean>> getAllDeferredResults(Message msg) {
        return msg.getTopics().stream()
                .map(topicToSubscribers::get)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(ISubscriber::address)
                .map(address -> CompletableFuture.supplyAsync(() -> sender.send(address, msg.getData()), pool))
                .collect(Collectors.toList());
    }

    private boolean checkResults(List<CompletableFuture<Boolean>> allResults) {
        return allResults.stream()
                .allMatch(future -> {
                    try {
                        return future.get();
                    } catch (ExecutionException ee) {
                        return false;
                    } catch (InterruptedException ie) {
                        throw new IllegalStateException("Thread was interrupted.", ie);
                    }
                });
    }
}
