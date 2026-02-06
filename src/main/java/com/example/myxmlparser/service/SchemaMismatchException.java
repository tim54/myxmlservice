package com.example.myxmlparser.service;

public class SchemaMismatchException extends RuntimeException {
    public SchemaMismatchException(String message) {
        super(message);
    }
}
