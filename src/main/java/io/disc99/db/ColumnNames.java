package io.disc99.db;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;

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

    public int size() {
        return values.size();
    }

    public ColumnName by(Integer index) {
        return values.get(index);
    }

    public List<ColumnName> all() {
        return values;
    }

//    public static ColumnNames of(List<Column> columns) {
//        return columns.stream()
//                .map(ColumnName::of)
//                .collect(Functions.toListAnd(ColumnNames::new));
//    }


}
