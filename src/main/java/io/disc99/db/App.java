package io.disc99.db;

import lombok.AllArgsConstructor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static io.disc99.db.Functions.toListAnd;

/*
 * Ubiquitous language.
 *
 * Table: Table
 * Column: Column
 * Row: row
 * Value: row data
 * Result: query rows
 *
 */
public class App {

    public static void main(String[] args) {

    }

    static Schema schema = Schema.create();

    static class Query {
        static ResultSet from(String tableName){
            return schema.get(tableName).toResultSet();
        }
        static ResultSet from(ResultSet resultSet){
            return resultSet;
        }
    }

    interface ResultSet {
        ResultSet select(String... columnNames);

        ResultSet leftJoin(String tableName, String matchingField);

        ResultSet leftJoin(ResultSet relation, String matchingField);

        ResultSet lessThan(String columnName, Integer value);

        ResultSet equalsTo(String columnName, Object value);
    }

    @AllArgsConstructor
    static class Result implements ResultSet {
        Columns columns;
        Rows rows;

        public Result(Columns columns) {
            this.columns = columns;
            this.rows = Rows.empty();
        }

        @Override
        public ResultSet select(String... columnNames) {
            Columns newColumns = Columns.create(columnNames);
            List<Integer> indexes = columns.findIndexesBy(newColumns);
            Rows newRows = rows.selectBy(indexes);
            return new Result(newColumns, newRows);
        }

        @Override
        public ResultSet leftJoin(String tableName, String matchingField) {
            Table tbl = schema.get(tableName);
            return leftJoin(tbl.toResultSet(), matchingField);
        }

        @Override
        public ResultSet leftJoin(ResultSet target, String matchingField) {
            Result tbl = (Result) target;
            Columns newColumns = columns.concat(tbl.columns);
            Column matchingColumn = new Column(matchingField);
            Integer leftColumnIdx = columns.findIndexBy(matchingColumn);

            if (!columns.exist(matchingColumn) && !tbl.columns.exist(matchingColumn)) {
                return new Result(newColumns);
            }

            Rows newRows = rows.map((Row row) -> {
                Object leftValue = row.get(leftColumnIdx);
                Result leftRel = (Result) target.equalsTo(matchingField, leftValue);
                return leftRel.isEmpty()
                        ? row.concat(Row.empty(newColumns.size() - row.size()))
                        : row.concat(leftRel.get(0)); // TODO current one to one only
            });
            return new Result(newColumns, newRows);
        }

        @Override
        public ResultSet lessThan(String columnName, Integer value) {
            Column column = new Column(columnName);
            if (!columns.exist(column)) {
                return new Result(columns);
            }
            Integer idx = columns.findIndexBy(column);
            Rows newRows = rows.lessThan(idx, value);
            return new Result(columns, newRows);
        }

        @Override
        public ResultSet equalsTo(String columnName, Object value) {
            Column column = new Column(columnName);
            if (!columns.exist(column)) {
                return new Result(columns);
            }
            Integer idx = columns.findIndexBy(column);
            Rows newRows = rows.equalsTo(idx, value);
            return new Result(columns, newRows);
        }

        private Row get(Integer index) {
            return rows.get(index);
        }

        private boolean isEmpty() {
            return rows.isEmpty();
        }

        @Override
        public String toString() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            //フィールド名
            for(Column cl : columns.values){
                pw.print("|");
                pw.print(cl.name);
            }
            pw.println("|");
            // データ
            for(Row row : rows.values){
                for(Object v : row.values){
                    pw.print("|");
                    pw.print(v);
                }
                pw.println("|");
            }

            return sw.toString();
        }
    }

    static class Table {
        String name;
        Columns columns;
        Rows rows;

        private Table(String name, Columns columns) {
            this.name = name;
            this.columns = columns;
            this.rows = Rows.empty();
        }

        static Table create(String name, String[] columnNames) {
            Table table = Arrays.stream(columnNames)
                    .map(Column::new)
                    .collect(toListAnd(columns -> new Table(name, new Columns(columns))));
            schema.add(table);
            return table;
        }

        Table insert(Object... values) {
            rows.add(new Row(Arrays.asList(values)));
            return this;
        }

        public Result toResultSet() {
            return new Result(columns, rows);
        }
    }

}

