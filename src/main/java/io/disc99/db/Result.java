package io.disc99.db;

import io.disc99.db.util.Collections;
import io.disc99.db.value.Value;
import lombok.ToString;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.List;

import static io.disc99.db.util.Functions.toListAnd;

@ToString
public class Result {
    ColumnNames names;
    Rows rows;

    private Result(ColumnNames names, Rows rows) {
        this.names = names;
        this.rows = rows;
    }

    public static Result of(ColumnNames names, Rows rows) {
        return new Result(names, rows);
    }

    public Integer columnSize() {
        return names.size();
    }

    public Result select(PlainSelect statement) {
        ColumnNames names = statement.getSelectItems().stream()
                .map(selectItem -> ColumnName.of((net.sf.jsqlparser.schema.Column) ((SelectExpressionItem) selectItem).getExpression()))
                .collect(toListAnd(ColumnNames::of));

        List<Integer> indexes = this.names.indexes(names);
        Rows extractedRows = rows.extract(indexes);

        return Result.of(names, extractedRows);
    }

    public Result concat(Result result) {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Current support below syntax only.
     *
     * <p>
     *  left join {TABLE} on {COLUMN_NAME} = {COLUMN_NAME}
     * </p>
     *
     * @param result
     * @param rightColumn
     * @param leftColumn
     * @param join
     * @return
     */
    // TODO Supports other expressions
    public Result join(Result result, ColumnName rightColumn, ColumnName leftColumn, Join join) {
        Integer rightIndex = names.index(rightColumn);
        Integer leftIndex = result.names.index(leftColumn);

        Rows rows = this.rows.all().stream()
                .map(row -> {
                    Value rightValue = row.by(rightIndex);
                    Rows rightRows = result.rows.equalTo(leftIndex, rightValue);
                    if (rightRows.isEmpty() && join.isLeft()) {
                        rightRows = Rows.of(Row.empty(result.columnSize())); // TODO immutable variable
                    }

                    return row.combine(rightRows);
                }).reduce(Rows::add).orElse(Rows.empty());

        ColumnNames columnNames = ColumnNames.of(Collections.concat(names.all(), result.names.all()));

        return Result.of(columnNames, rows);
    }

    public Result limit(Integer limit) {
        Rows rows = this.rows.limit(limit);
        return Result.of(names, rows);
    }
}
