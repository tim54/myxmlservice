package com.example.myxmlparser.domain;

import lombok.Getter;

@Getter
public enum SqlType {
    INT("integer"),
    BIGINT("bigint"),
    VARCHAR("varchar"),
    DECIMAL("decimal"),
    DATE("date"),
    TIMESTAMP("timestamp"),
    BOOLEAN("boolean");

    private final String sql;

    SqlType(String sql) {
        this.sql = sql;
    }

}