package io.disc99.db.value;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class StringValue implements Value<String> {
    String value;

    @Override
    public int compareTo(String o) {
        return value.compareTo(o);
    }

    @Override
    public String value() {
        return value;
    }
}
