package io.disc99.db;


public class Table {
    TableName name;
    Columns columns;
    Rows rows;

    private Table(TableName name, Columns columns) {
        this.name = name;
        this.columns = columns;
        this.rows = Rows.empty();
    }

    public static Table of(TableName name, Columns columns) {
        return new Table(name, columns);
    }

    public TableName name() {
        return name;
    }

    public void insert(Row row) {
        rows.add(row);
    }
}
