package io.disc99.db;

import io.disc99.db.value.Value;
import lombok.ToString;

import java.util.List;

import static io.disc99.db.Functions.toListAnd;

@ToString
public class Row {
    List<Value> values;

    private Row(List<Value> values) {
        this.values = values;
    }

    public static Row of(List<Value> values) {
        return new Row(values);
    }

    public Row extract(List<Integer> indexes) {
        return indexes.stream()
                .map(index -> values.get(index))
                .collect(toListAnd(Row::of));
    }
}
