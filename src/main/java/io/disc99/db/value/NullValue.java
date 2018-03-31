package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@ToString
public class NullValue implements Value<Object> {

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public Object value() {
        return null;
    }
}
