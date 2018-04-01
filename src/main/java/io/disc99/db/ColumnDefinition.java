package io.disc99.db;

import lombok.ToString;

@ToString
public class ColumnDefinition {
    String name;
    Type type;

    private ColumnDefinition(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public static ColumnDefinition of(net.sf.jsqlparser.statement.create.table.ColumnDefinition definition) {
        return new ColumnDefinition(definition.getColumnName(), Type.of(definition.getColDataType().getDataType()));
    }

    public boolean sameAs(ColumnName name) {
        return this.name.equals(name.value);
    }

    public String value() {
        return name;
    }
}
