package com.example.myxmlparser.service;

import com.example.myxmlparser.domain.Table;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DbUpdateService {

    private final XmlParserService xmlParserService;
    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    public void update() {
        for (String tableName : xmlParserService.getTableNames()) {
            update(tableName);
        }
    }

    public void update(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }

        Table xmlDef = xmlParserService.getTableDefinition(tableName);
        assertSchemaSameOrThrow(tableName, xmlDef);

        List<Map<String, Object>> rows = xmlParserService.getTableRows(tableName);

        for (Map<String, Object> row : rows) {
            Object id = row.get("id");
            if (id == null || String.valueOf(id).isBlank()) {
                throw new IllegalArgumentException("В XML нет обязательного поля/атрибута id для таблицы: " + tableName);
            }

            Map<String, Object> updatable = new LinkedHashMap<>(row);
            updatable.remove("id");

            if (updatable.isEmpty()) {
                continue;
            }

            // фильтруем только те колонки, которые есть в XML-описании
            Set<String> allowedCols = xmlDef.getColumns().stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            updatable.entrySet().removeIf(e -> !allowedCols.contains(e.getKey()));

            if (updatable.isEmpty()) {
                continue;
            }

            String setClause = updatable.keySet().stream()
                    .map(this::quoteIdentifier)
                    .map(c -> c + " = ?")
                    .collect(Collectors.joining(", "));

            String sql = "UPDATE " + quoteIdentifier(tableName) + " SET " + setClause + " WHERE id = ?";

            List<Object> args = new ArrayList<>(updatable.values());
            args.add(id);

            jdbcTemplate.update(sql, args.toArray());
        }
    }

    private void assertSchemaSameOrThrow(String tableName, Table xmlDef) {
        // Ожидаемые колонки из XML
        // Разрешаем created_at в БД, даже если её нет в XML.
        Set<String> expected = xmlDef.getColumns().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> actual = readTableColumns(tableName);

        Set<String> actualMinusService = new LinkedHashSet<>(actual);
        actualMinusService.remove("created_at");

        if (!actualMinusService.equals(expected)) {
            throw new SchemaMismatchException(
                    "Структура таблицы в БД отличается от XML для '" + tableName + "'.\n" +
                            "XML: " + expected + "\n" +
                            "DB: " + actual + "\n"
            );
        }
    }

    private Set<String> readTableColumns(String tableName) {
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData meta = c.getMetaData();

            try (ResultSet rs = meta.getColumns(null, "public", tableName, null)) {
                LinkedHashSet<String> cols = new LinkedHashSet<>();
                while (rs.next()) {
                    cols.add(rs.getString("COLUMN_NAME"));
                }
                if (cols.isEmpty()) {
                    throw new SchemaMismatchException("Таблица не найдена в БД или нет колонок: " + tableName);
                }
                return cols;
            }
        } catch (SchemaMismatchException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Не удалось прочитать структуру таблицы из БД: " + tableName, e);
        }
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}