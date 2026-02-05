package com.example.myxmlparser.service;

import com.example.myxmlparser.domain.SqlType;
import com.example.myxmlparser.domain.Table;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.NodeChild;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

@Service
public class XmlParserService {

    private static final Pattern SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

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

    public List<String> getTableNames(GPathResult document) {
        if (document == null) throw new IllegalArgumentException("document не должен быть null");

        Object shopObj = document.getProperty("shop");
        if (!(shopObj instanceof GPathResult shop)) {
            return List.of();
        }

        GPathResult children = shop.children();

        Set<String> names = new LinkedHashSet<>();
        for (Object child : children) {
            if (child instanceof GPathResult childNode) {
                String nodeName = childNode.name();
                if (nodeName == null || nodeName.isBlank()) {
                    continue;
                }

                boolean hasElementChildren = false;
                for (Object grandChild : childNode.children()) {
                    if (grandChild instanceof GPathResult) {
                        hasElementChildren = true;
                        break;
                    }
                }

                String text = childNode.text();
                boolean hasNoText = text == null || text.trim().isEmpty();

                if (hasElementChildren || hasNoText) {
                    names.add(nodeName);
                }
            }
        }
        return new ArrayList<>(names);
    }

    public void parseXML(GPathResult document) {
        if (document == null) throw new IllegalArgumentException("document не должен быть null");

        Object shopObj = document.getProperty("shop");
        if (!(shopObj instanceof GPathResult shop)) {
            return;
        }

        GPathResult children = shop.children();

        Set<String> names = new LinkedHashSet<>();
        for (Object child : children) {
            if (child instanceof GPathResult childNode) {
                String nodeName = childNode.name();
                if (nodeName == null || nodeName.isBlank()) {
                    continue;
                }

                boolean hasElementChildren = false;
                List<Table> tables = new ArrayList<>();

                for (Object grandChild : childNode.children()) {
                    if (grandChild instanceof GPathResult grandChildNode) {

                        Table table = new Table();
                        table.setName(nodeName);

                        Map<String, String> attrs = getAttributes(grandChildNode);

                        String text = grandChildNode.text();
                        text = (text == null) ? "" : text.trim();

                        System.out.println("node: " + grandChildNode.name());
                        if (!attrs.isEmpty()) {
                            System.out.println("attributes: " + attrs);
                        }
                        if (!text.isEmpty()) {
                            System.out.println("text: " + text);
                        }
                        System.out.println();

                        List<Map.Entry<String, SqlType>> columns = new ArrayList<>();
                        for (Map.Entry<String, String> attr : attrs.entrySet()) {
                            Map.Entry<String, SqlType> column = new AbstractMap.SimpleEntry<>(attr.getKey(), detect(attr.getValue()));
                            System.out.println(column);
                            columns.add(column);
                        }

                        table.setColumns(columns);
                        tables.add(table);
                    }
                }

                String text = childNode.text();
                boolean hasNoText = text == null || text.trim().isEmpty();

                if (hasElementChildren || hasNoText) {
                    names.add(nodeName);
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

    public Map<String, String> getAttributes(GPathResult node) {
        if (node == null) throw new IllegalArgumentException("node не должен быть null");

        if (!(node instanceof NodeChild nodeChild)) {
            // If this is NodeChildren / other GPathResult, we don't have a single node to read attributes from
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
        if (!SQL_IDENTIFIER.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Некорректное имя таблицы: " + tableName);
        }

        String t = quoteIdentifier(tableName);

        return """
               CREATE TABLE IF NOT EXISTS %s (
                   id BIGSERIAL PRIMARY KEY,
                   source VARCHAR(1024),
                   payload XML NOT NULL,
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
               );
               """.formatted(t);
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
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
