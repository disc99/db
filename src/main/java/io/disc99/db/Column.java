package io.disc99.db;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Column {
    ColumnName name;
    Type type;

    private Column(ColumnName name, Type type) {
        this.name = name;
        this.type = type;
    }

//    public static Column of(String name, Type type) {
//        return new Column(name, type);
//    }

    public static Column of(ColumnDefinition definition) {
        return new Column(ColumnName.of(definition.getColumnName()), Type.of(definition.getColDataType().getDataType()));
    }


    public ColumnName name() {
        return name;
    }
}
