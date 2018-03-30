package io.disc99.db;

import io.disc99.db.value.IntegerValue;
import io.disc99.db.value.Value;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;

import java.math.BigInteger;
import java.sql.Timestamp;

import static io.disc99.db.Functions.toListAnd;

/**
 * Factory of domain model from JSQLParser object.
 */
public class Factory {

    public static Column createColumn(ColumnDefinition definition) {
        return Column.of(definition.getColumnName(), Type.of(definition.getColDataType().getDataType()));
    }

    public static TableName createTableName(CreateTable createTable) {
        return TableName.of(createTable.getTable().getName());
    }

    public static TableName createTableName(Insert insert) {
        return TableName.of(insert.getTable().getName());
    }

    public static Row createRow(Insert insert) {
        ExpressionList expressionList = (ExpressionList) insert.getItemsList();
        return expressionList.getExpressions().stream()
                .map(Factory::createValue)
                .collect(toListAnd(Row::of));
    }

    public static Value createValue(Expression expression) {
        if (expression instanceof LongValue) {
            BigInteger value = ((LongValue) expression).getBigIntegerValue();
            return new IntegerValue(value.intValue());
        }
        if (expression instanceof StringValue) {
            String value = ((StringValue) expression).getValue();
            return new io.disc99.db.value.StringValue(value);
        }
        if (expression instanceof DoubleValue) {
            Double value = ((DoubleValue) expression).getValue();
            return new io.disc99.db.value.DoubleValue(value);
        }
        if (expression instanceof TimestampValue) {
            Timestamp value = ((TimestampValue) expression).getValue();
            return new io.disc99.db.value.TimestampValue(value);
        }
        throw new SqlException("Unsupported data expression: " + expression);
    }
}
