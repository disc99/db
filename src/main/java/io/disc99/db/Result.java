package io.disc99.db;

import lombok.ToString;

@ToString
public class Result {
    ColumnNames names;
    Rows rows;

    private Result(ColumnNames names, Rows rows) {
        this.names = names;
        this.rows = rows;
    }

    public static Result of(ColumnNames names, Rows rows) {
        return new Result(names, rows);
    }
}
