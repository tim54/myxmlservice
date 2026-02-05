package com.example.myxmlparser.domain;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter @Setter
public class Table {
    String name;
    List<Map.Entry<String, SqlType>> columns;
}
