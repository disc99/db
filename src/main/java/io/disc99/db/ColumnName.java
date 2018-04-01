package io.disc99.db;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;
import net.sf.jsqlparser.schema.Column;

@ToString
@EqualsAndHashCode
public class ColumnName {
    TableName table;
    String value;

    private ColumnName(TableName table, String value) {
        this.table = table;
        this.value = value;
    }

    public static ColumnName of(TableName table, String name) {
        return new ColumnName(table, name);
    }

    public static ColumnName of(Column column) {
        return new ColumnName(TableName.of(column.getTable()), column.getColumnName());
    }

    public boolean sameAs(ColumnName name) {
        if (name.table.isEmpty()) {
            return value.equals(name.value);
        }
        return table.equals(name.table) && value.equals(name.value);
    }
}
