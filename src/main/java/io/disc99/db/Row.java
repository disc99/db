package io.disc99.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static io.disc99.db.Functions.toListAnd;

class Row {
    List<Object> values;

    public Row(List<Object> values) {
        this.values = values;
    }

    public static Row empty(Integer size) {
        return new Row(Arrays.asList(new Object[size]));
    }

    public Row selectBy(List<Integer> indexes) {
        return indexes.stream()
                .map(i -> values.get(i))
                .collect(toListAnd(Row::new));
    }

    public boolean equalsTo(Integer index, Object value) {
        return values.get(index).equals(value);
    }

    public boolean lessThan(Integer index, Integer value) {
        return (Integer)values.get(index) < value;
    }

    public Row concat(Row row) {
        return Stream.concat(values.stream(), row.values.stream())
                .collect(toListAnd(Row::new));
    }

    public Object get(Integer index) {
        return values.get(index);
    }

    public Integer size() {
        return values.size();
    }
}
