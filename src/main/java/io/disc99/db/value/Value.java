package io.disc99.db.value;

public interface Value<T> extends Comparable<T> {
    T value();
}
