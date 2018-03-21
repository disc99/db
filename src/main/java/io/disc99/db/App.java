package io.disc99.db;

import lombok.AllArgsConstructor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/*
 * Ubiquitous language.
 *
 * Table: Table
 * Column: Column
 * Record: Table Data
 *
 * Result: selected record
 * Row: selected one result
 *
 */
public class App {

    public static void main(String[] args) {

    }

    static Map<String, Table> scheme = new HashMap<>();


    static class Relation {
        List<Column> columns;
        List<Record> records;

        int findColumn(String name) {
            for(int i = 0; i < columns.size(); ++i){
                if(columns.get(i).name.equals(name)){
                    return i;
                }
            }
            return columns.size();
        }

        @Override
        public String toString() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            //フィールド名
            for(Column cl : columns){
                pw.print("|");
                if(cl.parent != null && !cl.parent.isEmpty()){
                    pw.print(cl.parent + ".");
                }
                pw.print(cl.name);
            }
            pw.println("|");
            //データ
            for(Record record : records){
                for(Object v : record.values){
                    pw.print("|");
                    pw.print(v);
                }
                pw.println("|");
            }

            return sw.toString();
        }
    }

    static class Query extends Relation {
        private Query(List<Column> columns, List<Record> records) {
            this.columns = columns;
            this.records = records;
        }

        static Query from(String tableName) {
            Table table = scheme.get(tableName);
            return scheme.get(tableName).columns.stream()
                    .map(column -> new Column(tableName, column.name))
                    .collect(collectingAndThen(toList(), columns -> new Query(columns, table.records)));
        }

        Query select(String... columnNames) {
            List<Integer> indexes = new ArrayList<>();
            List<Column> newColumns = new ArrayList<>();
            for (String n : columnNames) {
                newColumns.add(new Column(n));
                //属性を探す
                int idx = findColumn(n);
                indexes.add(idx);
            }
            //データの投影
            List<Record> newRecords = new ArrayList<>();
            for (Record record : records) {
                List<Object> values = new ArrayList<>();
                for (int idx : indexes) {
                    if (idx < record.values.size()) {
                        values.add(record.values.get(idx));
                    } else {
                        values.add(null);
                    }
                }
                newRecords.add(new Record(values));
            }

            return new Query(newColumns, newRecords);
        }

        Query leftJoin(String tableName, String matchingField) {
            //テーブル取得
            Table tbl = scheme.get(tableName);
            //属性の作成
            List<Column> newColumns = new ArrayList<>(columns);
            for(Column cl : tbl.columns){
                newColumns.add(new Column(tableName, cl.name));
            }

            //値の作成
            List<Record> newRecords = new ArrayList<>();
            int leftColumnIdx = findColumn(matchingField);
            int rightColumnIdx = tbl.findColumn(matchingField);
            if(leftColumnIdx >= columns.size() || rightColumnIdx >= tbl.columns.size()){
                //該当フィールドがない場合は値の結合をしない
                return new Query(newColumns, Collections.EMPTY_LIST);
            }
            //結合処理
            for(Record tp : records){
                //元のテーブルのデータ
                Record ntpl = new Record(new ArrayList<>(tp.values));
                //足りないフィールドを埋める
                while(ntpl.values.size() < columns.size()){
                    ntpl.values.add(null);
                }
                //結合対象のフィールドを探す
                Object leftValue = ntpl.values.get(leftColumnIdx);
                //左側テーブルに値があるときだけ結合
                if(leftValue != null){
                    for(Record rtpl : tbl.records){
                        if(rtpl.values.size() < rightColumnIdx){
                            continue;
                        }
                        if(leftValue.equals(rtpl.values.get(rightColumnIdx))){
                            //一致するとき
                            for(Object v : rtpl.values){
                                ntpl.values.add(v);
                            }
                            break;//今回は、タプルの対応は一対一まで
                        }
                    }
                }
                //足りないフィールドを埋める
                while(ntpl.values.size() < newColumns.size()){
                    ntpl.values.add(null);
                }

                newRecords.add(ntpl);
            }
            return new Query(newColumns, newRecords);
        }

        Query lessThan(String columnName, Integer value){
            int idx = findColumn(columnName);
            if(idx >= columns.size()){
                return new Query(columns, Collections.EMPTY_LIST);
            }
            List<Record> newRecords = new ArrayList<>();
            for(Record tp : records){
                if((Integer)tp.values.get(idx) < value){
                    newRecords.add(tp);
                }
            }
            return new Query(columns, newRecords);
        }

    }
    static class Table extends Relation {
        String name;
        private Table(String name, List<Column> columns) {
            this.name = name;
            this.columns = columns;
            records = new ArrayList<>();
        }

        static Table create(String name, String[] columnNames) {
            Table table = Arrays.stream(columnNames)
                    .map(Column::new)
                    .collect(collectingAndThen(toList(), columns -> new Table(name, columns)));
            scheme.put(name, table);
            return table;
        }

        Table insert(Object... values) {
            records.add(new Record(Arrays.asList(values)));
            return this;
        }
    }
    
    @AllArgsConstructor
    static class Record {
        List<Object> values;
    }

    @AllArgsConstructor
    static class Column {
        String parent;
        String name;
        Column(String name) {
            this("", name);
        }
    }
}
