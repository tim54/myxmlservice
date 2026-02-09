package com.example.myxmlparser.utility;

import com.example.myxmlparser.service.DbUpdateService;
import com.example.myxmlparser.service.XmlParserService;
import groovy.xml.slurpersupport.GPathResult;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class TestExerciseUtility {

    public static void doExercise(XmlParserService xmlParserService, GPathResult doc, DbUpdateService dbUpdateService){
        xmlParserService.parseXML(doc);

        for (String tableName : xmlParserService.getTableNames()) {
            xmlParserService.getTableDDL(tableName); // categories, offers
        }

//        dbUpdateService.dropAllTables(false);

//        dbUpdateService.create();
        dbUpdateService.update();

//        List<String> tableNames = xmlParserService.getTableNames(doc);
//
//        log.info("Название таблицы: " + tableNames);
//
//        for (String tableName : tableNames) {
//            log.info("Обработка: " + tableName);
//
//            String sqlDDL = xmlParserService.getTableDDL(tableName);
//
//            log.info("sqlDDL: " + sqlDDL);
//        }
    }
}
