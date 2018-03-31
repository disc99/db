package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class DoubleValue implements Value<Double> {
    Double value;

    @Override
    public int compareTo(Double o) {
        return value.compareTo(o);
    }

    @Override
    public Double value() {
        return value;
    }
}
