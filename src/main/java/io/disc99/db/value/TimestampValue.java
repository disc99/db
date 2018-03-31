package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.sql.Timestamp;

@AllArgsConstructor
@ToString
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
