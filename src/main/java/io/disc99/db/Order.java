package io.disc99.db;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class Order {
    // Only one column
    ColumnName name;
    boolean asc;

    private Order(ColumnName name, boolean asc) {
        this.name = name;
        this.asc = asc;
    }

    public static Order of(OrderByElement element) {
        return new Order(ColumnName.of((Column) element.getExpression()), element.isAsc());
    }

    public ColumnName name() {
        return name;
    }

    public boolean isAsc() {
        return asc;
    }
}
