package io.disc99.db.syakyo;

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
