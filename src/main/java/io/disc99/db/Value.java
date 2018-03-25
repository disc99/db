package io.disc99.db;

public interface Value<T> extends Comparable<T> {
    T value();
}
