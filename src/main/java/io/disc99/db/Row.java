package io.disc99.db;

import io.disc99.db.value.Value;

import java.util.List;

public class Row {
    List<Value> values;

    private Row(List<Value> values) {
        this.values = values;
    }

    public static Row of(List<Value> values) {
        return new Row(values);
    }
}
