package ru.company.nano;

import java.io.Serializable;

@FunctionalInterface
public interface ISender {
    /**
     * It doesn't throw exceptions.
     * @return true if sending was successful, false otherwise.
     */
    boolean send(String address, Serializable data);
}
