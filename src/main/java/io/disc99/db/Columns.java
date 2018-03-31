package io.disc99.db;


import lombok.ToString;

import java.util.List;
import java.util.stream.IntStream;

import static io.disc99.db.Functions.toListAnd;
import static java.util.stream.Collectors.toList;

@ToString
public class Columns {
    List<Column> values;

    private Columns(List<Column> columns) {
        this.values = columns;
    }

    public static Columns of(List<Column> columns) {
        return columns.stream()
                .collect(toListAnd(Columns::new));
    }

    public Integer index(ColumnName name) {
        return IntStream.range(0, values.size())
                .filter(i -> values.get(i).name().equals(name))
                .boxed()
                .findFirst()
                .orElseThrow(() -> new SqlException("Unknown column: " + name));
    }

    public List<Integer> indexes(ColumnNames names) {
        return names.all().stream()
                .map(this::index)
                .collect(toList());
    }

    public ColumnNames names() {
        return values.stream()
                .map(Column::name)
                .collect(toListAnd(ColumnNames::of));
    }

//    public static Columns single(Column column) {
//        return new Columns(singletonList(column));
//    }
//
//    public List<Integer> findIndexesBy(Columns columns) {
//        return columns.values.stream()
//                .map(this::findIndexBy)
//                .collect(toList());
//    }
//
//    public Integer findIndexBy(Column column) {
//        return IntStream.range(0, values.size())
//                .filter(i -> values.get(i).equals(column))
//                .findFirst().orElseThrow(() -> new RuntimeException("Not found column: " + column));
//    }
//
//    public Columns concat(Columns columns) {
//        return Stream.concat(this.values.stream(), columns.values.stream())
//                .collect(toListAnd(Columns::new));
//    }
//
//    public boolean exist(Column column) {
//        return values.stream()
//                .anyMatch(col -> col.equals(column));
//    }
//
    public Integer size() {
        return values.size();
    }
}
