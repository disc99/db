package io.disc99.db;

import java.util.Arrays;

public enum Type {
    TEXT("text"),
    INTEGER("integer"),
    REAL("real"),
    TIMESTAMP("timestamp"),
    ;
    String[] dataTypes;

    Type(String... dataTypes) {
        this.dataTypes = dataTypes;
    }

    public static Type of(String dataType) {
        return Arrays.stream(values())
                .filter(val -> Arrays.stream(val.dataTypes).anyMatch(type -> type.equals(dataType)))
                .findFirst().orElseThrow(() -> new SqlException("Unsupported data type: " + dataType));
    }
}
