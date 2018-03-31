package io.disc99.db;

import lombok.Value;
import lombok.experimental.Accessors;
import net.sf.jsqlparser.schema.Column;

public class ColumnName {
    String value;

    private ColumnName(String value) {
        this.value = value;
    }

    public static ColumnName of(Column column) {
        return new ColumnName(column.getColumnName());
    }
}
