package io.disc99.db;

import lombok.ToString;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

@ToString
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
        return new Column(ColumnName.of(definition), Type.of(definition.getColDataType().getDataType()));
    }


    public ColumnName name() {
        return name;
    }
}
