package io.disc99.db;

import io.disc99.db.value.NullValue;
import io.disc99.db.value.Value;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.disc99.db.util.Collections.concat;
import static io.disc99.db.util.Functions.toListAnd;

@ToString
public class Row {
    List<Value> values;

    private Row(List<Value> values) {
        this.values = values;
    }

    public static Row empty(Integer size) {
        return IntStream.range(0, size)
                .mapToObj(i -> new NullValue())
                .collect(toListAnd(Row::of));
    }

    public static Row of(List<Value> values) {
        return new Row(values);
    }

    public Row extract(List<Integer> indexes) {
        return indexes.stream()
                .map(index -> values.get(index))
                .collect(toListAnd(Row::of));
    }

    public Value by(Integer index) {
        return values.get(index);
    }

    public Rows leftJoin(Rows rows) {
        return rows.all().stream()
                .map(row -> Row.of(concat(values, row.values)))
                .collect(toListAnd(Rows::of));
    }
}
