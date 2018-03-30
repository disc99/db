package io.disc99.db;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.disc99.db.Functions.toListAnd;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class Columns {
    List<Column> values;

    private Columns(List<Column> columns) {
        this.values = columns;
    }

    public static Columns of(List<Column> columns) {
        return columns.stream()
                .collect(toListAnd(Columns::new));
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
//    public Integer size() {
//        return values.size();
//    }
}
