package com.example.myxmlparser.service;

import com.example.myxmlparser.domain.SqlType;
import com.example.myxmlparser.domain.Table;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DbUpdateService {

    private final XmlParserService xmlParserService;
    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    /**
     * Создает таблицы в БД на основании XML
     * Если таблица уже существует — проверяет структуру
     * При отличиях кидает SchemaMismatchException.
     */
    public void create() {
        for (String tableName : xmlParserService.getTableNames()) {
            create(tableName);
        }
    }

    /**
     * Создает таблицу в БД на основании XML.
     * Если таблица уже существует — проверяет структуру
     * При отличиях кидает SchemaMismatchException.
     * @param tableName имя таблицы из XML
     */
    public void create(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }

        Table xmlDef = xmlParserService.getTableDefinition(tableName);

        if (tableExists(tableName)) {
            assertSchemaSameOrThrow(tableName, xmlDef);
            return; // уже создана и структура совпадает
        }

        String ddl = xmlParserService.getTableDDL(tableName);
        jdbcTemplate.execute(ddl);
    }

    /**
     * обновляет данные в таблицах бд
     * на основе Id
     * если поменялась структура выдает exception
     */
    public void update() {
        for (String tableName : xmlParserService.getTableNames()) {
            update(tableName);
        }
    }

    /**
     * обновляет данные в таблицах бд
     * если поменялась структура выдает exception
     * @param tableName
     */
    public void update(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }

        Table xmlDef = xmlParserService.getTableDefinition(tableName);
        assertSchemaSameOrThrow(tableName, xmlDef);

        List<Map<String, Object>> rows = xmlParserService.getTableRows(tableName);

        Map<String, SqlType> columnTypes = xmlDef.getColumns().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));

        if (!columnTypes.containsKey("id")) {
            throw new IllegalArgumentException("В XML-описании таблицы нет обязательной колонки id: " + tableName);
        }

        // фильтруем только те колонки, которые есть в XML-описании (включая id)
        Set<String> allowedCols = xmlDef.getColumns().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        for (Map<String, Object> row : rows) {
            Object rawId = row.get("id");
            if (rawId == null || String.valueOf(rawId).isBlank()) {
                throw new IllegalArgumentException("В XML нет обязательного атрибута id для таблицы: " + tableName);
            }

            Object id = coerceValue(tableName, "id", columnTypes.get("id"), rawId);

            Map<String, Object> updatable = new LinkedHashMap<>(row);
            updatable.entrySet().removeIf(e -> !allowedCols.contains(e.getKey()));
            updatable.remove("id");

            Map<String, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : updatable.entrySet()) {
                String col = e.getKey();
                SqlType type = columnTypes.get(col);
                converted.put(col, coerceValue(tableName, col, type, e.getValue()));
            }

            // UPSERT pattern:
            // INSERT ... ON CONFLICT (id) DO UPDATE SET col = EXCLUDED.col ...
            // If there are no updatable columns, do nothing on conflict.
            List<String> insertCols = new ArrayList<>();
            insertCols.add("id");
            insertCols.addAll(converted.keySet());

            String colsClause = insertCols.stream()
                    .map(this::quoteIdentifier)
                    .collect(Collectors.joining(", "));

            String valuesClause = insertCols.stream()
                    .map(c -> "?")
                    .collect(Collectors.joining(", "));

            String conflictAction;
            if (converted.isEmpty()) {
                conflictAction = "DO NOTHING";
            } else {
                String updateSetClause = converted.keySet().stream()
                        .map(this::quoteIdentifier)
                        .map(c -> c + " = EXCLUDED." + c)
                        .collect(Collectors.joining(", "));
                conflictAction = "DO UPDATE SET " + updateSetClause;
            }

            String sql = "INSERT INTO " + quoteIdentifier(tableName)
                    + " (" + colsClause + ")"
                    + " VALUES (" + valuesClause + ")"
                    + " ON CONFLICT (" + quoteIdentifier("id") + ") "
                    + conflictAction;

            List<Object> args = new ArrayList<>();
            args.add(id);
            args.addAll(converted.values());

            System.out.println(sql);

            jdbcTemplate.update(sql, args.toArray());
        }
    }

    private Object coerceValue(String tableName, String column, SqlType type, Object raw) {
        if (raw == null) {
            return null;
        }

        if (raw instanceof String s && s.isBlank()) {
            return null;
        }

        if (type == null) {
            return raw;
        }

        try {
            return switch (type) {
                case INT -> {
                    if (raw instanceof Integer i) yield i;
                    if (raw instanceof Number n) yield n.intValue();
                    yield Integer.parseInt(raw.toString().trim());
                }
                case BIGINT -> {
                    if (raw instanceof Long l) yield l;
                    if (raw instanceof Number n) yield n.longValue();
                    yield Long.parseLong(raw.toString().trim());
                }
                case DECIMAL -> {
                    if (raw instanceof BigDecimal bd) yield bd;
                    if (raw instanceof Number n) yield BigDecimal.valueOf(n.doubleValue());
                    yield new BigDecimal(raw.toString().trim().replace(',', '.'));
                }
                case BOOLEAN -> {
                    if (raw instanceof Boolean b) yield b;
                    String s = raw.toString().trim().toLowerCase(Locale.ROOT);
                    if (s.equals("1") || s.equals("true") || s.equals("t") || s.equals("yes")) yield true;
                    if (s.equals("0") || s.equals("false") || s.equals("f") || s.equals("no")) yield false;
                    yield Boolean.parseBoolean(s);
                }
                case DATE -> {
                    if (raw instanceof LocalDate d) yield d;
                    yield LocalDate.parse(raw.toString().trim());
                }
                case TIMESTAMP -> {
                    if (raw instanceof LocalDateTime dt) yield dt;
                    // Accept ISO-8601 like "2024-01-31T12:34:56"
                    yield LocalDateTime.parse(raw.toString().trim());
                }
                case VARCHAR -> raw.toString();
            };
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Не удалось преобразовать значение для " + tableName + "." + column
                            + " к типу " + type.getSql() + ": raw=" + raw + " (" + raw.getClass().getName() + ")",
                    ex
            );
        }
    }

    /**
     * Удаляетвсе таблицы в схеме.
     * @param cascade если true — удалит зависимые объекты
     */
    public void dropAllTables(boolean cascade) {
        dropAllTablesInSchema("public", cascade);
    }

    /**
     * Удаляет (DROP) все таблицы в указанной схеме.
     */
    public void dropAllTablesInSchema(String schema, boolean cascade) {
        if (schema == null || schema.isBlank()) {
            throw new IllegalArgumentException("schema не должен быть пустым");
        }

        List<String> tableNames = listTables(schema);

        // Чтобы меньше страдать от зависимостей, можно дропать в обратном порядке (не всегда нужно, но помогает).
        Collections.reverse(tableNames);

        for (String tableName : tableNames) {
            dropTable(schema, tableName, cascade);
        }
    }

    /**
     * Удаляет (DROP) только указанные таблицы.
     * Имена таблиц должны быть "логическими" (без кавычек); кавычки добавляются автоматически.
     */
    public void dropTables(Collection<String> tableNames, boolean cascade) {
        dropTablesInSchema("public", tableNames, cascade);
    }

    /**
     * Удаляет (DROP) только указанные таблицы в указанной схеме.
     */
    public void dropTablesInSchema(String schema, Collection<String> tableNames, boolean cascade) {
        if (schema == null || schema.isBlank()) {
            throw new IllegalArgumentException("schema не должен быть пустым");
        }
        if (tableNames == null) {
            throw new IllegalArgumentException("tableNames не должен быть null");
        }

        for (String tableName : tableNames) {
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("tableName не должен быть пустым");
            }
            dropTable(schema, tableName, cascade);
        }
    }

    /**
     * DROP TABLE IF EXISTS "schema"."table" [CASCADE]
     */
    private void dropTable(String schema, String tableName, boolean cascade) {
        String sql = "DROP TABLE IF EXISTS "
                + quoteQualifiedIdentifier(schema, tableName)
                + (cascade ? " CASCADE" : "");

        jdbcTemplate.execute(sql);
    }

    private List<String> listTables(String schema) {
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData meta = c.getMetaData();
            try (ResultSet rs = meta.getTables(null, schema, null, new String[]{"TABLE"})) {
                List<String> names = new ArrayList<>();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_NAME"));
                }
                return names;
            }
        } catch (Exception e) {
            throw new RuntimeException("Не удалось получить список таблиц в схеме: " + schema, e);
        }
    }

    private String quoteQualifiedIdentifier(String schema, String identifier) {
        return quoteIdentifier(schema) + "." + quoteIdentifier(identifier);
    }

    private boolean tableExists(String tableName) {
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData meta = c.getMetaData();

            try (ResultSet rs = meta.getTables(null, "public", tableName, new String[]{"TABLE"})) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new RuntimeException("Не удалось проверить существование таблицы: " + tableName, e);
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
                            "DB: " + actualMinusService + "\n"
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
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}