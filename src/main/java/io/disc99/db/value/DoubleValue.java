package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class DoubleValue implements Value<Double> {
    Double value;

    @Override
    public int compareTo(Value o) {
        return value.compareTo(((DoubleValue)o).value());
    }

    @Override
    public Double value() {
        return value;
    }
}
