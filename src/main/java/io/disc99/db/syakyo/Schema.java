package io.disc99.db.syakyo;

import java.util.HashMap;
import java.util.Map;
import io.disc99.db.syakyo.App.*;

/**
 * Schema.
 */
public class Schema {
    Map<String, Table> tables;

    private Schema() {
        tables = new HashMap<>();
    }

    public static Schema create() {
        return new Schema();
    }

    public Schema add(Table table) {
        tables.put(table.name, table);
        return this;
    }

    public Table get(String tableName) {
        return tables.get(tableName);
    }
}
