package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class IntegerValue implements Value<Integer> {
    Integer value;

    @Override
    public int compareTo(Integer o) {
        return value.compareTo(o);
    }

    @Override
    public Integer value() {
        return value;
    }
}
