package io.disc99.db;

import io.disc99.db.engine.relation.SqlParser;
import io.disc99.db.syakyo.Rows;
import lombok.AllArgsConstructor;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.util.Map;

import static io.disc99.db.Functions.toListAnd;


@AllArgsConstructor
public class Database {

    SqlParser sqlParser;
    Map<TableName, Table> tables;

    public void execute(String sql) {
        Statement statement = sqlParser.parse(sql);

        if (statement instanceof CreateTable) {
            CreateTable createTable = (CreateTable) statement;
            Table table = createTable.getColumnDefinitions().stream()
                    .map(Factory::createColumn)
                    .collect(toListAnd(columns -> Table.of(Factory.createTableName(createTable), Columns.of(columns))));
            tables.put(table.name(), table);

        } else if (statement instanceof CreateIndex) {
            // TODO

        } else if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            Table table = tables.get(Factory.createTableName(insert));
            table.insert(Factory.createRow(insert));

        } else if (statement instanceof Update) {
            // TODO

        } else if (statement instanceof Delete) {
            // TODO

        }

        throw new SqlException("Unknown statement: " + statement);
    }

    public Rows query(String sql) {
        Statement statement = sqlParser.parse(sql);
        if (!(statement instanceof Select)) {
            throw new SqlException("Unknown statement: " + statement);
        }
        // TODO
        return null;
    }
}
