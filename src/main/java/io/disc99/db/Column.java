package io.disc99.db;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
class Column {
    String name;

    Column(String name) {
        this.name = name;
    }
}
