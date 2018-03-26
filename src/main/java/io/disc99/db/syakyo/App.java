package io.disc99.db.syakyo;

import lombok.AllArgsConstructor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static io.disc99.db.syakyo.Functions.toListAnd;

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

        ResultSet equalsTo(String columnName, Value value);

        ResultSet orderBy(String columnName);

        ResultSet orderBy(String columnName, boolean asc);

        ResultSet groupBy(String columnName, Aggregation... aggregations);
    }

    static class Aggregations {
        List<Aggregation> values;
        private Aggregations(Aggregation[] values) {
            this.values = Arrays.asList(values);
        }

        static Aggregations of(Aggregation... aggregations) {
            return new Aggregations(aggregations);
        }

        public Columns columns() {
            return values.stream()
                    .map(Aggregation::name)
                    .collect(toListAnd(Columns::new));
        }
    }

    public static abstract class Aggregation {
        String columnName;
        public Aggregation(String columnName) {
            this.columnName = columnName;
        }
        public abstract Column name();
        public abstract Column column();
        public abstract void add(Value value);
        public abstract Object result();
        public abstract void reset();
    }

    public static class Count extends Aggregation {
        int counter = 0;

        @Override
        public Column name() {
            return new Column("count");
        }

        @Override
        public Column column() {
            return new Column(columnName);
        }

        public Count(String columnName) {
            super(columnName);
        }
        @Override
        public void add(Value value) {
            ++counter;
        }
        @Override
        public Object result() {
            return counter;
        }
        @Override
        public void reset() {
            counter = 0;
        }
    }

    public static class Average extends Aggregation {
        int counter = 0;
        int total = 0;

        public Average(String columnName) {
            super(columnName);
        }

        @Override
        public Column name() {
            return new Column("average");
        }

        @Override
        public Column column() {
            return new Column(columnName);
        }

        @Override
        public void add(Value value) {
            ++counter;
            total += (Integer) value.value();
        }

        @Override
        public Object result() {
            return (double) total / counter;
        }

        @Override
        public void reset() {
            counter = 0;
            total = 0;
        }
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
                Value leftValue = row.get(leftColumnIdx);
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
        public ResultSet equalsTo(String columnName, Value value) {
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
        @Override
        public ResultSet orderBy(String columnName) {
            return orderBy(columnName, true);
        }

        @Override
        public ResultSet orderBy(String columnName, final boolean asc) {
            final int idx = columns.findIndexBy(new Column(columnName));
            if(idx >= columns.size()){
                return this;
            }

            Rows newRows = rows.sort(idx, asc);
//            List<Row> newRow = new ArrayList<>(rows);
//            Collections.sort(newRow, new Comparator<Row>(){
//                @Override
//                public int compare(Row o1, Row o2) {
//                    Object v1 = o1.values.size() > idx ? o1.values.get(idx) : null;
//                    Object v2 = o2.values.size() > idx ? o2.values.get(idx) : null;
//                    if(v1 == null){
//                        if(v2 == null){
//                            return 0;
//                        }else{
//                            return 1;
//                        }
//                    }
//                    if(v2 == null){
//                        return -1;
//                    }
//                    return ((Comparable)v1).compareTo(v2) * (asc ? 1 : -1);
//                }
//            });
            return new Result(columns, newRows);
        }

        @Override
        public ResultSet groupBy(String columnName, Aggregation... aggregations) {
//            Column column = new Column(columnName);
//            Aggregations aggs = Aggregations.of(aggregations);
//            //列名を作成
//            Columns aggColumns = aggs.columns();
//            List<Integer> colIndexes = columns.findIndexesBy(aggColumns);
//
////            List<Column> newColumns = new ArrayList<>();
//            Columns newColumns = Columns.single(column).concat(aggColumns);
////            newColumns.add(column);
////            List<Integer> colIndexes = new ArrayList<>();
////            for(Aggregation agg : aggregations) {
////                newColumns.add(agg.name());
////                colIndexes.add(columns.findIndexBy(agg.column()));
////            }
//
//            //集計行を取得
//            int idx = columns.findIndexBy(column);
//            if (idx >= columns.size()) {
//                return new Result(newColumns);
//            }
//
//            //あらかじめソート
//            Result sorted = (Result) orderBy(columnName);
//
//            Value current = null;
//            List<Row> newRows = new ArrayList<>();
//
//            for (Row row : sorted.rows) {
//                //集計フィールド取得
//                Value v = row.values.get(idx);
//                if(v == null) continue;
//
//                if(!v.equals(current)){
//                    if(current != null){
//                        //集計行を追加
//                        List<Value> values = new ArrayList<>();
//                        values.add(current);
//                        for(Aggregation agg : aggregations){
//                            values.add(agg.result());
//                        }
//                        newRows.add(new Row(values));
//                    }
//                    current = v;
//                    for(Aggregation agg : aggregations){
//                        agg.reset();
//                    }
//                }
//
//                //集計
//                for(int i = 0; i < aggregations.length; ++i){
//                    int aidx = colIndexes.get(i);
//                    if(row.values.size() <= aidx) continue;
//                    Object cv = row.values.get(aidx);
//                    if(cv == null) continue;
//
//                    Aggregation ag = aggregations[i];
//                    ag.add(cv);
//                }
//            }
//
//            if(current != null){
//                //集計行を追加
//                List<Object> values = new ArrayList<>();
//                values.add(current);
//                for(Aggregation agg : aggregations){
//                    values.add(agg.result());
//                }
//                newRows.add(new Row(values));
//            }
//
//            return new Result(newColumns, newRows);
            return null;
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
//            rows.add(new Row(Arrays.asList(values)));
            return this;
        }

        public Result toResultSet() {
            return new Result(columns, rows);
        }
    }

}

