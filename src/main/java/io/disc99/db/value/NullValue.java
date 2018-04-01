package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class NullValue implements Value<Object> {

    @Override
    public int compareTo(Value o) {
        return 0;
    }

    @Override
    public Object value() {
        return null;
    }
}
