package io.disc99.db.syakyo;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Column {
    String name;

    Column(String name) {
        this.name = name;
    }
}
