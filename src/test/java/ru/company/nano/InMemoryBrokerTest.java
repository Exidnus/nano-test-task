package ru.company.nano;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InMemoryBrokerTest {
    private static final String TOPIC_ONE = "topic1";
    private final ISender mockSender = mock(ISender.class);
    private final IBroker inMemoryBroker = new InMemoryBroker(mockSender, 1_000L, 3);
    private final StubData stubData = new StubData();

    @Test
    void shouldSendMessageToAllRecipients() {
        inMemoryBroker.subscribe(new StubSubscriber("192.168.100.45"), TOPIC_ONE);
        inMemoryBroker.subscribe(new StubSubscriber("192.168.10.15"), TOPIC_ONE);
        inMemoryBroker.subscribe(new StubSubscriber("192.168.40.10"), "topic2");

        inMemoryBroker.send(new Message(TOPIC_ONE, stubData));

        ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Serializable> dataCaptor = ArgumentCaptor.forClass(Serializable.class);
        verify(mockSender, times(2)).send(addressCaptor.capture(), dataCaptor.capture());

        assertEquals(Set.of("192.168.100.45", "192.168.10.15"), new HashSet<>(addressCaptor.getAllValues()));
        assertEquals(stubData, dataCaptor.getValue());
    }

    @Test
    void shouldSentMessagesWhenSubscribedAndNotSentWhenNotSubscribed() {
        when(mockSender.send(anyString(), any())).thenReturn(true);

        StubSubscriber subscriber = new StubSubscriber("192.168.100.45");
        inMemoryBroker.subscribe(subscriber, TOPIC_ONE);
        assertTrue(inMemoryBroker.send(new Message(TOPIC_ONE, stubData)));

        inMemoryBroker.unsubscribe(subscriber, TOPIC_ONE);
        assertFalse(inMemoryBroker.send(new Message(TOPIC_ONE, stubData)));
    }

    @Test
    void shouldReturnFalseIfTimeoutExpired() {
        when(mockSender.send(anyString(), any())).then(invocation -> {
            Thread.sleep(2_000L);
            return true;
        });

        inMemoryBroker.subscribe(new StubSubscriber("192.168.100.45"), TOPIC_ONE);
        assertFalse(inMemoryBroker.send(new Message(TOPIC_ONE, stubData)));
    }

    private static class StubSubscriber implements ISubscriber {
        private static int idCounter = 0;
        private final String id;
        private final String address;

        StubSubscriber(String address) {
            this.id = String.valueOf(idCounter++);
            this.address = address;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String address() {
            return address;
        }
    }

    private static class StubData implements Serializable {

    }
}
