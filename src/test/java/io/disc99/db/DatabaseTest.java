package io.disc99.db;

import io.disc99.db.engine.relation.SqlParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseTest {

    @Test
    public void execute() {
        Database database = new Database(new SqlParser(), new HashMap<>());
        database.execute("create table books (id integer, name text)");
        database.execute("insert into books (id, name) values (1, 'book 1')");
        database.execute("insert into books (id, name) values (2, null)");

        assertThat(database.toString()).isEqualTo(
                "Database(" +
                    "tables={TableName(value=books)=Table(name=TableName(value=books), " +
                    "columnDefinitions=ColumnDefinitions(values=[ColumnDefinition(name=id, type=INTEGER), ColumnDefinition(name=name, type=TEXT)]), " +
                    "rows=Rows(values=[" +
                        "Row(values=[IntegerValue(value=1), StringValue(value=book 1)]), " +
                        "Row(values=[IntegerValue(value=2), NullValue()])" +
                    "]))}" +
                ")");
    }

    @Test
    public void query() {
        Database database = new Database(new SqlParser(), new HashMap<>());
        database.execute("create table books (id integer, name text)");
        database.execute("insert into books (id, name) values (1, 'book 1')");
        database.execute("insert into books (id, name) values (2, null)");
        database.execute("insert into books (id, name) values (3, 'book 3')");
        database.execute("create table users (id integer, book_id integer, name text)");
        database.execute("insert into users (id, book_id, name) values (1, 3, 'user 1')");
        database.execute("insert into users (id, book_id, name) values (2, 3, 'user 2')");

        Result result1 = database.query("select id, name from books");
        assertThat(result1.toString()).isEqualTo(
                "Result(" +
                    "names=ColumnNames(values=[" +
                        "ColumnName(table=TableName(value=null), value=id), " +
                        "ColumnName(table=TableName(value=null), value=name)]), " +
                    "rows=Rows(values=[" +
                        "Row(values=[IntegerValue(value=1), StringValue(value=book 1)]), " +
                        "Row(values=[IntegerValue(value=2), NullValue()]), " +
                        "Row(values=[IntegerValue(value=3), StringValue(value=book 3)])" +
                    "]))");

        Result result2 = database.query(
                "select books.id, users.id, books.name, users.name from books left join users on books.id = users.book_id");
        assertThat(result2.toString()).isEqualTo(
                "Result(" +
                    "names=ColumnNames(values=[" +
                        "ColumnName(table=TableName(value=books), value=id), " +
                        "ColumnName(table=TableName(value=users), value=id), " +
                        "ColumnName(table=TableName(value=books), value=name), " +
                        "ColumnName(table=TableName(value=users), value=name)]), " +
                    "rows=Rows(values=[" +
                        "Row(values=[IntegerValue(value=1), NullValue(), StringValue(value=book 1), NullValue()]), " +
                        "Row(values=[IntegerValue(value=2), NullValue(), NullValue(), NullValue()]), " +
                        "Row(values=[IntegerValue(value=3), IntegerValue(value=1), StringValue(value=book 3), StringValue(value=user 1)]), " +
                        "Row(values=[IntegerValue(value=3), IntegerValue(value=2), StringValue(value=book 3), StringValue(value=user 2)])" +
                    "]))");

        Result result3 = database.query(
                "select books.id, users.id, books.name, users.name from books inner join users on books.id = users.book_id");
        assertThat(result3.toString()).isEqualTo(
                "Result(" +
                    "names=ColumnNames(values=[" +
                        "ColumnName(table=TableName(value=books), value=id), " +
                        "ColumnName(table=TableName(value=users), value=id), " +
                        "ColumnName(table=TableName(value=books), value=name), " +
                        "ColumnName(table=TableName(value=users), value=name)]), " +
                    "rows=Rows(values=[" +
                        "Row(values=[IntegerValue(value=3), IntegerValue(value=1), StringValue(value=book 3), StringValue(value=user 1)]), " +
                        "Row(values=[IntegerValue(value=3), IntegerValue(value=2), StringValue(value=book 3), StringValue(value=user 2)])" +
                    "]))");
    }

    @Test
    public void a() throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse("select * from books b join users u on b.id = u.book_id");
        PlainSelect selectBody = (PlainSelect) ((Select)statement).getSelectBody();
        System.out.println("selectBody.getLimit(): " + selectBody.getLimit());
        System.out.println("selectBody.getJoins(): " + selectBody.getJoins());
        System.out.println("selectBody.getIntoTables(): " + selectBody.getIntoTables());
        System.out.println("selectBody.getHaving(): " + selectBody.getHaving());
        System.out.println("selectBody.getGroupByColumnReferences(): " + selectBody.getGroupByColumnReferences());
        System.out.println("selectBody.getForUpdateTable(): " + selectBody.getForUpdateTable());
        System.out.println("selectBody.getFirst(): " + selectBody.getFirst());
        System.out.println("selectBody.getFetch(): " + selectBody.getFetch());
        System.out.println("selectBody.getDistinct(): " + selectBody.getDistinct());
        System.out.println("selectBody.getFromItem(): " + selectBody.getFromItem());
        System.out.println("selectBody.getOffset(): " + selectBody.getOffset());
        System.out.println("selectBody.getOrderByElements(): " + selectBody.getOrderByElements());
        System.out.println("selectBody.getSelectItems(): " + selectBody.getSelectItems());
        System.out.println("selectBody.getWait(): " + selectBody.getWait());
        System.out.println("selectBody.getTop(): " + selectBody.getTop());
        System.out.println("selectBody.getWhere(): " + selectBody.getWhere());
    }
}