package io.disc99.db.value;

import io.disc99.db.SqlException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import java.math.BigInteger;
import java.sql.Timestamp;

public interface Value<T> extends Comparable<T> {

    T value();

    static Value of(Expression expression) {
        if (expression instanceof net.sf.jsqlparser.expression.NullValue) {
            return new NullValue();
        }
        if (expression instanceof LongValue) {
            BigInteger value = ((LongValue) expression).getBigIntegerValue();
            return new IntegerValue(value.intValue());
        }
        if (expression instanceof net.sf.jsqlparser.expression.StringValue) {
            String value = ((net.sf.jsqlparser.expression.StringValue) expression).getValue();
            return new io.disc99.db.value.StringValue(value);
        }
        if (expression instanceof net.sf.jsqlparser.expression.DoubleValue) {
            Double value = ((net.sf.jsqlparser.expression.DoubleValue) expression).getValue();
            return new io.disc99.db.value.DoubleValue(value);
        }
        if (expression instanceof net.sf.jsqlparser.expression.TimestampValue) {
            Timestamp value = ((net.sf.jsqlparser.expression.TimestampValue) expression).getValue();
            return new io.disc99.db.value.TimestampValue(value);
        }
        throw new SqlException("Unsupported data expression: " + expression);
    }
}
