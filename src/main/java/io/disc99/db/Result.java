package io.disc99.db;

import io.disc99.db.util.Collections;
import io.disc99.db.value.Value;
import lombok.ToString;
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
     * @return
     */
    // TODO Supports other expressions
    public Result leftJoin(Result result, ColumnName rightColumn, ColumnName leftColumn) {
        Integer rightIndex = names.index(rightColumn);
        Integer leftIndex = result.names.index(leftColumn);

        // 全columnに対する結合
        Rows rows = this.rows.all().stream()
                .map(row -> {
                    Value rightValue = row.by(rightIndex);
                    Rows rightRows = result.rows.all().stream()
                            .filter(r -> rightValue.equals(r.by(leftIndex))) // ターゲットのRowの絞り込み
                            .collect(toListAnd(Rows::of));
                    return row.leftJoin(rightRows.isEmpty() ? Rows.of(Row.empty(result.columnSize())) : rightRows);
                }).reduce(Rows::add).orElse(Rows.empty());

        ColumnNames columnNames = ColumnNames.of(Collections.concat(names.all(), result.names.all()));

        return Result.of(columnNames, rows);
    }
}
