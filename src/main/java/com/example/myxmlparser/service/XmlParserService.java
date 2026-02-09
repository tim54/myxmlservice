package com.example.myxmlparser.service;

import com.example.myxmlparser.domain.SqlType;
import com.example.myxmlparser.domain.Table;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.NodeChild;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

@Service
@Slf4j
public class XmlParserService {

    private final List<Table> tables = new ArrayList<>();
    private final List<String> tableNames = new ArrayList<>();

    private GPathResult lastDocument;

    public GPathResult readFromFile(Path xmlPath) {
        if (xmlPath == null) throw new IllegalArgumentException("xmlPath не должен быть null");

        try (InputStream in = Files.newInputStream(xmlPath)) {
            return slurper().parse(in);
        } catch (Exception e) {
            throw new XmlParsingException("Ошибка парсинга XML файла: " + xmlPath, e);
        }
    }

    public GPathResult readFromUrl(URL url) {
        if (url == null) throw new IllegalArgumentException("xmlPath не должен быть null");

        try (InputStream in = url.openStream()) {
            return slurper().parse(in);
        } catch (Exception e) {
            throw new XmlParsingException("Ошибка парсинга XML URL: " + url, e);
        }
    }

    public GPathResult readFromUrl(String url) {
        if (url == null || url.isBlank()) throw new IllegalArgumentException("url не должен быть пустым");
        try {
            return readFromUrl(URI.create(url).toURL());
        } catch (Exception e) {
            throw new XmlParsingException("Ошибка парсинга XML URL: " + url, e);
        }
    }

    /**
     * Возвращает названия таблиц из XML (currency, categories, offers)
     * @return ArrayList
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    public void parseXML(GPathResult document) {
        if (document == null) throw new IllegalArgumentException("document не должен быть null");

        this.lastDocument = document;

        tables.clear();
        tableNames.clear();

        Object shopObj = document.getProperty("shop");
        if (!(shopObj instanceof GPathResult shop)) {
            return;
        }

        GPathResult children = shop.children();

        for (Object child : children) {
            if (child instanceof GPathResult childNode) {
                String nodeName = childNode.name();
                if (nodeName == null || nodeName.isBlank()) {
                    continue;
                }

                if (!childNode.children().isEmpty()) {
                    Table table = new Table();
                    table.setName(nodeName);

                    List<Map.Entry<String, SqlType>> columns = new ArrayList<>();
                    for (Object grandChild : childNode.children()) {
                        if (grandChild instanceof GPathResult grandChildNode) {

                            Map<String, String> attrs = getAttributes(grandChildNode);

                            String text = grandChildNode.text();
                            text = (text == null) ? "" : text.trim();

                            for (Map.Entry<String, String> attr : attrs.entrySet()) {
                                Map.Entry<String, SqlType> column =
                                        new AbstractMap.SimpleEntry<>(attr.getKey().toLowerCase(), detect(attr.getValue()));

                                if (columns.contains(column)) {
                                    continue;
                                }
                                columns.add(column);
                            }

                            boolean hasKids = hasElementChildren(grandChildNode);
                            boolean hasText = grandChildNode.text() != null && !grandChildNode.text().trim().isEmpty();
                            if (hasKids) {
                                int paramIndex = 0;

                                for (Object grandGrandChild : grandChildNode.children()) {
                                    if (grandGrandChild instanceof GPathResult grandGrandChildNode) {
                                        String name1 = grandGrandChildNode.name();

                                        if (name1.equals("param")) {
                                            name1 = "param_" + paramIndex++;
                                        }

                                        Map.Entry<String, SqlType> column =
                                                new AbstractMap.SimpleEntry<>(name1.toLowerCase(), SqlType.VARCHAR);

                                        if (columns.contains(column)) {
                                            continue;
                                        }
                                        columns.add(column);
                                    }
                                }
                            } else if (hasText) {
                                String name1 = grandChildNode.name();
                                String text1 = grandChildNode.text();

                                Map.Entry<String, SqlType> column =
                                        new AbstractMap.SimpleEntry<>(name1, detect(text1));

                                if (columns.contains(column)) {
                                    continue;
                                }
                                columns.add(column);
                            }
                        }
                    }
                    table.setColumns(columns);
                    tables.add(table);
                    tableNames.add(table.getName());
                }
            }
        }
    }

    private SqlType detect(String v) {
        if (isBoolean(v)) return SqlType.BOOLEAN;
        if (isInt(v)) return SqlType.INT;
        if (isLong(v)) return SqlType.BIGINT;
        if (isDecimal(v)) return SqlType.DECIMAL;
        if (isDate(v)) return SqlType.DATE;
        if (isTimestamp(v)) return SqlType.TIMESTAMP;
        return SqlType.VARCHAR;
    }

    boolean isBoolean(String v) {
        return v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false");
    }

    boolean isInt(String v) {
        return v.matches("-?\\d{1,9}");
    }

    boolean isLong(String v) {
        return v.matches("-?\\d{10,18}");
    }

    boolean isDecimal(String v) {
        return v.matches("-?\\d+\\.\\d+");
    }

    boolean isDate(String v) {
        return v.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    boolean isTimestamp(String v) {
        return v.matches("\\d{4}-\\d{2}-\\d{2}T.*");
    }

    boolean hasElementChildren(GPathResult node) {
        for (Object ch : node.children()) {
            if (ch instanceof NodeChild) {
                return true; // found an element child
            }
        }
        return false;
    }

    public Map<String, String> getAttributes(GPathResult node) {
        if (node == null) throw new IllegalArgumentException("node не должен быть null");

        if (!(node instanceof NodeChild nodeChild)) {
            return Map.of();
        }

        Object raw = nodeChild.attributes();
        if (!(raw instanceof Map<?, ?> rawMap) || rawMap.isEmpty()) {
            return Map.of();
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : rawMap.entrySet()) {
            if (e.getKey() != null) {
                result.put(String.valueOf(e.getKey()), e.getValue() == null ? null : String.valueOf(e.getValue()));
            }
        }
        return result;
    }

    /**
     * Создает sql для создания таблиц динамически из XML
     * @param tableName имя таблицы (например: currencies, categories, offers)
     * @return SQL DDL (PostgreSQL)
     */
    public String getTableDDL(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }

        Table table = tables.stream().filter(t -> t.getName().equals(tableName)).findFirst().orElse(null);
        if (table == null) {
            throw new IllegalArgumentException("Таблица не найдена: " + tableName);
        }
        String t = quoteIdentifier(tableName);

        StringBuilder sqlDDLColumns = new StringBuilder();
        for (Map.Entry<String, SqlType> column : table.getColumns()) {

            if (column.getKey().equals("id"))
                sqlDDLColumns.append("id " + column.getValue().getSql() + " PRIMARY KEY,\n");
            else
                sqlDDLColumns.append(String.format("%s %s, \n", column.getKey(), column.getValue().getSql()));
        }

        String tmp = """
               CREATE TABLE IF NOT EXISTS %s (
                   %s
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
               );
               """.formatted(t, sqlDDLColumns);

        System.out.println(tmp);

        return """
               CREATE TABLE IF NOT EXISTS %s (
                   %s
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
               );
               """.formatted(t, sqlDDLColumns);
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Возвращает строки для tableName для обновления из последнего XML.
     * Обновление потом по ключу "id".
     */
    public List<Map<String, Object>> getTableRows(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }
        if (lastDocument == null) {
            throw new IllegalStateException("XML еще не распарсен: вызовите parseXML(document) перед getTableRows()");
        }

        Object shopObj = lastDocument.getProperty("shop");
        if (!(shopObj instanceof GPathResult shop)) {
            return List.of();
        }

        Object tableNodeObj = shop.getProperty(tableName);
        if (!(tableNodeObj instanceof GPathResult tableNode)) {
            return List.of();
        }

        List<Map<String, Object>> rows = new ArrayList<>();

        for (Object rowObj : tableNode.children()) {
            if (!(rowObj instanceof GPathResult rowNode)) {
                continue;
            }

            Map<String, Object> row = new LinkedHashMap<>();

            Map<String, String> attrs = getAttributes(rowNode);
            for (Map.Entry<String, String> a : attrs.entrySet()) {
                row.put(a.getKey().toLowerCase(), a.getValue());
            }

            int paramIndex = 0;
            boolean hasKids = hasElementChildren(rowNode);
            boolean hasText = rowNode.text() != null && !rowNode.text().trim().isEmpty();

            if (hasKids) {
                for (Object chObj : rowNode.children()) {
                    if (!(chObj instanceof GPathResult ch)) continue;

                    String colName = ch.name();
                    if ("param".equals(colName)) {
                        colName = "param_" + paramIndex++;
                    }

                    String value = ch.text();
                    value = value == null ? null : value.trim();

                    Map<String, String> childAttrs = getAttributes(ch);
                    for (Map.Entry<String, String> ca : childAttrs.entrySet()) {
                        row.putIfAbsent(ca.getKey().toLowerCase(), ca.getValue());
                    }

                    row.put(colName, value);
                }
            } else if (hasText) {
                String colName = rowNode.name();
                String value = rowNode.text().trim();
                row.put(colName, value);
            }

            if (!row.isEmpty()) {
                rows.add(row);
            }
        }

        return rows;
    }

    public Table getTableDefinition(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName не должен быть пустым");
        }
        return tables.stream()
                .filter(t -> tableName.equals(t.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Таблица не найдена: " + tableName));
    }

    private XmlSlurper slurper() throws Exception {

        XmlSlurper slurper = new XmlSlurper(false, true);

        slurper.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);

        slurper.setFeature("http://xml.org/sax/features/external-general-entities", false);
        slurper.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        slurper.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

        return slurper;
    }
}
