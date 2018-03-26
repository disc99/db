package io.disc99.db.syakyo;

public interface Value<T> extends Comparable<T> {
    T value();
}
