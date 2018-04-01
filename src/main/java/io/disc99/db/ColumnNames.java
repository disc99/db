package io.disc99.db;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@ToString
@EqualsAndHashCode
public class ColumnNames {
    List<ColumnName> values;

    private ColumnNames(List<ColumnName> values) {
        this.values = values;
    }
    public static ColumnNames of(List<ColumnName> columns) {
        return new ColumnNames(columns);
    }

    public Integer index(ColumnName name) {
        return IntStream.range(0, values.size())
                .filter(i -> values.get(i).sameAs(name))
                .boxed()
                .findFirst()
                .orElseThrow(() -> new SqlException("Unknown column: " + name));
    }

    public List<Integer> indexes(ColumnNames names) {
        return names.all().stream()
                .map(this::index)
                .collect(toList());
    }

    public int size() {
        return values.size();
    }

    public List<ColumnName> all() {
        return values;
    }
}
