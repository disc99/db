package io.disc99.db.value;

import lombok.AllArgsConstructor;

@AllArgsConstructor
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
