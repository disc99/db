package io.disc99.db;

import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

import static io.disc99.db.Functions.toListAnd;

@ToString
public class Rows {
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

    public Rows extract(List<Integer> indexes) {
        return values.stream()
                .map(row -> row.extract(indexes))
                .collect(toListAnd(Rows::new));
    }
}
