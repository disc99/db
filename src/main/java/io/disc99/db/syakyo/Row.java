package io.disc99.db.syakyo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static io.disc99.db.Functions.toListAnd;

public class Row {
    List<Value> values;

    public Row(List<Value> values) {
        this.values = values;
    }

    public static Row empty(Integer size) {
        return new Row(Arrays.asList(new Value[size]));
    }

    public Row selectBy(List<Integer> indexes) {
        return indexes.stream()
                .map(i -> values.get(i))
                .collect(toListAnd(Row::new));
    }

    public boolean equalsTo(Integer index, Value value) {
        return values.get(index).equals(value);
    }

    public boolean lessThan(Integer index, Integer value) {
        Object object = values.get(index).value();
        return object == null ? false : (Integer) object < value;
    }

    public Row concat(Row row) {
        return Stream.concat(values.stream(), row.values.stream())
                .collect(toListAnd(Row::new));
    }

    public Value get(Integer index) {
        return values.get(index);
    }

    public Integer size() {
        return values.size();
    }
}
