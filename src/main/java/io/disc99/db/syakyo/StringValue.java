package io.disc99.db.syakyo;

import lombok.AllArgsConstructor;

@AllArgsConstructor
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
