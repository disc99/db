package io.disc99.db;

import io.disc99.db.value.Value;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.Stack;
import java.util.function.Predicate;

import static io.disc99.db.util.Functions.toListAnd;

public class WhereParser implements ExpressionVisitor {


    Result result;
    Stack<Predicate<Row>> conditionStack;
    Stack<java.util.function.Function<Row, Value>> valueStack;

    public Result parse(Result result, Expression where) {
        this.result = result;
        this.conditionStack = new Stack<>();
        this.valueStack = new Stack<>();
        where.accept(this);
        Predicate<Row> condition = conditionStack.pop();

        return result.rows.all().stream()
                .filter(condition)
                .collect(toListAnd(rows -> Result.of(this.result.names, Rows.of(rows))));
    }

    @Override
    public void visit(NullValue nullValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Function function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(LongValue longValue) {
        valueStack.push(row -> new io.disc99.db.value.IntegerValue(longValue.getBigIntegerValue().intValue()));
    }

    @Override
    public void visit(HexValue hexValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(StringValue stringValue) {
        valueStack.push(row -> new io.disc99.db.value.StringValue(stringValue.getValue()));
    }

    @Override
    public void visit(Addition addition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Division division) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Multiplication multiplication) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
        Predicate<Row> left = conditionStack.pop();
        Predicate<Row> right = conditionStack.pop();
        conditionStack.push(left.and(right));
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        orExpression.getRightExpression().accept(this);
        Predicate<Row> left = conditionStack.pop();
        Predicate<Row> right = conditionStack.pop();
        conditionStack.push(left.or(right));
    }

    @Override
    public void visit(Between between) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getRightExpression().accept(this);
        equalsTo.getLeftExpression().accept(this);
        java.util.function.Function<Row, Value> left = valueStack.pop();
        java.util.function.Function<Row, Value> right = valueStack.pop();
        conditionStack.push(row -> left.apply(row).equals(right.apply(row)));
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MinorThan minorThan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Column tableColumn) {
        ColumnName name = ColumnName.of(tableColumn);
        Integer index = result.names.index(name);
        valueStack.push(row -> row.by(index));
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Concat concat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Matches matches) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CastExpression cast) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Modulo modulo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(UserVariable var) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(NumericBind bind) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(KeepExpression aexpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OracleHint hint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(NotExpression aThis) {
        throw new UnsupportedOperationException();
    }

}
