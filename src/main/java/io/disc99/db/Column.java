package io.disc99.db;

public class Column {
    String name;
    Type type;

    private Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public static Column of(String name, Type type) {
        return new Column(name, type);
    }
}
