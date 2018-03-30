package io.disc99.db;

import java.util.ArrayList;
import java.util.List;

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

}
