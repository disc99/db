package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class IntegerValue implements Value<Integer> {
    Integer value;

    @Override
    public int compareTo(Value o) {
        return value.compareTo(((IntegerValue)o).value());
    }

    @Override
    public Integer value() {
        return value;
    }
}
