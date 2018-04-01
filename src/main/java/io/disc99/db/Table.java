package io.disc99.db;


import io.disc99.db.value.Value;
import lombok.ToString;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.disc99.db.util.Functions.toListAnd;
import static java.util.stream.Collectors.toMap;

@ToString
public class Table {
    TableName name;
    ColumnDefinitions columnDefinitions;
    Rows rows;

    private Table(TableName name, ColumnDefinitions columnDefinitions) {
        this.name = name;
        this.columnDefinitions = columnDefinitions;
        this.rows = Rows.empty();
    }

    public static Table create(CreateTable createTable) {
        return createTable.getColumnDefinitions().stream()
                .map(ColumnDefinition::of)
                .collect(toListAnd(columns -> new Table(TableName.of(createTable), ColumnDefinitions.of(columns))));
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
                        i -> columnDefinitions.index(ColumnName.of(columnList.get(i))),
                        i -> Value.of(expressions.get(i))
                ));

        Row row = IntStream.range(0, columnDefinitions.size())
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

    public Result toResult() {
        return Result.of(columnDefinitions.asNames(name), rows);
    }
}
