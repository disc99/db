package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.sql.Timestamp;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class TimestampValue implements Value<Timestamp> {
    Timestamp value;

    @Override
    public int compareTo(Value o) {
        return value.compareTo(((TimestampValue)o).value());
    }

    @Override
    public Timestamp value() {
        return value;
    }
}
