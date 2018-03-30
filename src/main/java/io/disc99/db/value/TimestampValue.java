package io.disc99.db.value;

import lombok.AllArgsConstructor;

import java.sql.Timestamp;

@AllArgsConstructor
public class TimestampValue implements Value<Timestamp> {
    Timestamp value;

    @Override
    public int compareTo(Timestamp o) {
        return value.compareTo(o);
    }

    @Override
    public Timestamp value() {
        return value;
    }
}
