package io.disc99.db;

import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.disc99.db.util.Functions.toListAnd;
import static java.util.Collections.singletonList;

@ToString
public class Rows {
    List<Row> values;

    private Rows(List<Row> rows) {
        this.values = rows;
    }

    public static Rows empty() {
        return new Rows(new ArrayList<>());
    }

    public static Rows of(Row row) {
        return new Rows(singletonList(row));
    }

    public static Rows of(List<Row> rows) {
        return new Rows(rows);
    }

    public Rows add(Row row) {
        values.add(row);
        return this;
    }

    public Rows add(Rows rows) {
        values.addAll(rows.values);
        return this;
    }

    public Rows extract(List<Integer> indexes) {
        return values.stream()
                .map(row -> row.extract(indexes))
                .collect(toListAnd(Rows::new));
    }

    public List<Row> all() {
        return values;
    }

    boolean isEmpty() {
        return values.isEmpty();
    }
}
