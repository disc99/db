package io.disc99.db;


import lombok.ToString;

import java.util.List;
import java.util.stream.IntStream;

import static io.disc99.db.util.Functions.toListAnd;

@ToString
public class ColumnDefinitions {
    List<ColumnDefinition> values;

    private ColumnDefinitions(List<ColumnDefinition> columns) {
        this.values = columns;
    }

    public static ColumnDefinitions of(List<ColumnDefinition> columns) {
        return columns.stream()
                .collect(toListAnd(ColumnDefinitions::new));
    }

    public List<ColumnDefinition> all() {
        return values;
    }

    public Integer index(ColumnName name) {
        return IntStream.range(0, values.size())
                .filter(i -> values.get(i).sameAs(name))
                .boxed()
                .findFirst()
                .orElseThrow(() -> new SqlException("Unknown column: " + name));
    }

    public Integer size() {
        return values.size();
    }

    public ColumnNames asNames(TableName table) {
        return values.stream()
                .map(def -> ColumnName.of(table, def.value()))
                .collect(toListAnd(ColumnNames::of));
    }
}
