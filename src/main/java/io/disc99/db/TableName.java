package io.disc99.db;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

@ToString
@EqualsAndHashCode
public class TableName {
    String value;

    private TableName(String value) {
        this.value = value;
    }

    public static TableName of(CreateTable statement) {
        return new TableName(statement.getTable().getName());
    }

    public static TableName of(Table statement) {
        return new TableName(statement.getName());
    }

    public static TableName of(Insert statement) {
        return new TableName(statement.getTable().getName());
    }

    public static TableName of(Update statement) {
        return new TableName(statement.getTables().get(0).getName());
    }

    public static TableName of(Delete statement) {
        return new TableName(statement.getTable().getName());
    }

    public boolean isEmpty() {
        return value == null;
    }
}
