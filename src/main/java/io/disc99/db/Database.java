package io.disc99.db;

import io.disc99.db.engine.relation.SqlParser;
import io.disc99.db.syakyo.Rows;
import lombok.AllArgsConstructor;
import lombok.ToString;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
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
        net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table) select.getFromItem();
        Table selectTable = tables.get(TableName.of(table));
        return selectTable.select(select);
    }

    @Override
    public String toString() {
        return "Database(tables=" + tables.toString() + ")";
    }
}
