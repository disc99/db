package io.disc99.db;

import io.disc99.db.engine.relation.SqlParser;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Map;


@AllArgsConstructor
public class Database {

    SqlParser sqlParser;
    Map<TableName, Table> tables;

    public void execute(String sql) {
        Statement statement = sqlParser.parse(sql);

        if (statement instanceof CreateTable) {
            CreateTable createTable = (CreateTable) statement;
            Table table = Table.create(createTable);
            tables.put(table.name(), table);
            return;
        }

        if (statement instanceof CreateIndex) {
            // TODO
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            Table table = tables.get(TableName.of(insert));
            table.insert(insert);
            return;
        }

        if (statement instanceof Update) {
            Update update = (Update) statement;
            Table table = tables.get(TableName.of(update));
            table.update(update);
            return;
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            Table table = tables.get(TableName.of(delete));
            table.delete(delete);
            return;
        }

        throw new SqlException("Unknown statement: " + statement);
    }

    public Result query(String sql) {
        Statement statement = sqlParser.parse(sql);
        if (!(statement instanceof Select)) {
            throw new SqlException("Unknown statement: " + statement);
        }

        PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
        Result result = tables.get(TableName.of((net.sf.jsqlparser.schema.Table) select.getFromItem())).toResult();

        if (select.getJoins() != null) {
            result = select.getJoins().stream()
                    .reduce(result, (leftResult, join) -> {
                        net.sf.jsqlparser.schema.Table rightItem = (net.sf.jsqlparser.schema.Table) join.getRightItem();
                        EqualsTo equalsTo = (EqualsTo) join.getOnExpression();
                        Column leftExpression = (Column) equalsTo.getLeftExpression();
                        Column rightExpression = (Column) equalsTo.getRightExpression();
                        Result rightResult = tables.get(TableName.of(rightItem)).toResult();
                        return leftResult.join(rightResult, ColumnName.of(leftExpression), ColumnName.of(rightExpression), join);
                    }, Result::concat);
        }

        // where
        Expression where = select.getWhere();
        WhereParser whereParser = new WhereParser();
        if (where != null) {
            result = whereParser.parse(result, where);
        }

        result = result.select(select);

        if (select.getOrderByElements() != null) {
            List<OrderByElement> orderByElements = select.getOrderByElements();
            result = result.orderBy(Order.of(orderByElements.get(0)));
        }

        if (select.getLimit() != null && !select.getLimit().isLimitNull()) {
            result = result.limit(((LongValue)select.getLimit().getRowCount()).getBigIntegerValue().intValue());
        }

        return result;
    }

    @Override
    public String toString() {
        return "Database(tables=" + tables.toString() + ")";
    }
}
