package io.disc99.db;


import io.disc99.db.value.Value;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.disc99.db.Functions.toListAnd;
import static java.util.stream.Collectors.toMap;

public class Table {
    TableName name;
    Columns columns;
    Rows rows;

    private Table(TableName name, Columns columns) {
        this.name = name;
        this.columns = columns;
        this.rows = Rows.empty();
    }

    public static Table create(CreateTable createTable) {
        return createTable.getColumnDefinitions().stream()
                .map(Column::of)
                .collect(toListAnd(columns -> new Table(TableName.of(createTable), Columns.of(columns))));
    }

    public TableName name() {
        return name;
    }

    public void insert(Insert statement) {
        List<net.sf.jsqlparser.schema.Column> columnList = statement.getColumns();
        List<Expression> expressions = ((ExpressionList) statement.getItemsList()).getExpressions();

        Map<Integer, Value> valueIndex = IntStream.range(0, columnList.size())
                .boxed()
                .collect(toMap(
                        i -> columns.index(ColumnName.of(columnList.get(i))),
                        i -> Value.of(expressions.get(i))
                ));

        Row row = IntStream.range(0, columns.size())
                .mapToObj(valueIndex::get)
                .collect(toListAnd(Row::of));

        rows.add(row);
    }

    public void update(Update statement) {
        // TODO
    }

    public void delete(Delete statement) {
        // TODO
    }
}