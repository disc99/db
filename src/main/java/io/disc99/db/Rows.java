package io.disc99.db;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.disc99.db.Functions.toListAnd;

class Rows {
    List<Row> values;

    private Rows(List<Row> rows) {
        this.values = rows;
    }

    public static Rows empty() {
        return new Rows(new ArrayList<>());
    }

    public Rows add(Row row) {
        values.add(row);
        return this;
    }

    public Rows selectBy(List<Integer> indexes) {
        return values.stream()
                .map(row -> row.selectBy(indexes))
                .collect(toListAnd(Rows::new));
    }

    public Rows equalsTo(Integer index, Object value) {
        return values.stream()
                .filter(val -> val.equalsTo(index, value))
                .collect(toListAnd(Rows::new));
    }

    public Rows lessThan(Integer index, Integer value) {
        return values.stream()
                .filter(val -> val.lessThan(index, value))
                .collect(toListAnd(Rows::new));
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Row get(Integer index) {
        return values.get(index);
    }

    public Rows map(Function<Row, Row> mapper) {
        return values.stream()
                .map(mapper)
                .collect(toListAnd(Rows::new));
    }
}
