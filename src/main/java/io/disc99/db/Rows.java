package io.disc99.db;

import io.disc99.db.value.Value;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.disc99.db.util.Functions.toListAnd;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;

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
                .collect(toListAnd(Rows::of));
    }

    public Rows equalTo(Integer index, Value value) {
        return values.stream()
                .filter(row -> value.equals(row.by(index)))
                .collect(toListAnd(Rows::of));
    }

    public List<Row> all() {
        return values;
    }

    boolean isEmpty() {
        return values.isEmpty();
    }

    public Rows limit(Integer limit) {
        return Rows.of(values.subList(0, min(values.size(), limit)));
    }

    public Rows sort(Integer index, boolean asc) {
        Comparator<Row> comparator = comparing(row -> row.by(index));
        if (!asc) {
            comparator = comparator.reversed();
        }
        return values.stream()
                .sorted(comparator)
                .collect(toListAnd(Rows::of));
    }
}
