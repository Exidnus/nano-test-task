package ru.company.nano;

import java.io.Serializable;

/**
 * Stub for sending messages through network.
 */
@FunctionalInterface
public interface ISender {
    /**
     * @return true if sending was successful, false otherwise.
     */
    boolean send(String address, Serializable data);
}
