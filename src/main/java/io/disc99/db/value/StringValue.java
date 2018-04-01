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
    public int compareTo(Value o) {
        return value.compareTo(((StringValue)o).value());
    }

    @Override
    public String value() {
        return value;
    }
}
