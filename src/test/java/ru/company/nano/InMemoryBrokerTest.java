package ru.company.nano;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

class InMemoryBrokerTest {
    private final ISender mockSender = Mockito.mock(ISender.class);
    private final IBroker inMemoryBroker = new InMemoryBroker(mockSender, 2_000L, 3);

    @Test
    void shouldSendMessageToAllRecipients() {
        inMemoryBroker.subscribe(new StubSubscriber("192.168.100.45"), "topic1");
        inMemoryBroker.subscribe(new StubSubscriber("192.168.10.15"), "topic1");
        inMemoryBroker.subscribe(new StubSubscriber("192.168.40.10"), "topic2");

        StubData stubData = new StubData();
        inMemoryBroker.send(new Message(Collections.singletonList("topic1"), stubData));

        ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Serializable> dataCaptor = ArgumentCaptor.forClass(Serializable.class);
        Mockito.verify(mockSender, Mockito.times(2)).send(addressCaptor.capture(), dataCaptor.capture());

        Assertions.assertEquals(new HashSet<>(Arrays.asList("192.168.100.45", "192.168.10.15")), new HashSet<>(addressCaptor.getAllValues()));
        Assertions.assertEquals(stubData, dataCaptor.getValue());
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
