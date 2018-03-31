package io.disc99.db;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

@ToString
@EqualsAndHashCode
public class ColumnName {
    String value;

    private ColumnName(String value) {
        this.value = value;
    }

    public static ColumnName of(Column column) {
        return new ColumnName(column.getColumnName());
    }

    public static ColumnName of(ColumnDefinition definition) {
        return new ColumnName(definition.getColumnName());
    }
}
