package com.example.myxmlparser;

import com.example.myxmlparser.utility.TestExerciseUtility;
import com.example.myxmlparser.service.XmlParserService;
import groovy.xml.slurpersupport.GPathResult;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.net.URL;
import java.nio.file.Path;

@SpringBootApplication
public class MyxmlparserApplication {

	public static void main(String[] args) {
		SpringApplication.run(MyxmlparserApplication.class, args);
	}

    @Bean
    ApplicationRunner xmlParserRunner(XmlParserService xmlParserService) {
        return args -> {

            String fileArg = args.getOptionValues("xml.file") == null ? null : args.getOptionValues("xml.file").getFirst();
            String urlArg = args.getOptionValues("xml.url") == null ? null : args.getOptionValues("xml.url").getFirst();

            GPathResult doc;

            if (fileArg != null && !fileArg.isBlank()) {
                doc = xmlParserService.readFromFile(Path.of(fileArg));
            } else if (urlArg != null && !urlArg.isBlank()) {
                doc = xmlParserService.readFromUrl(new URL(urlArg));
            } else {
                System.out.println("Входные данные отсутствуют. Используйте --xml.file=... или --xml.url=...");
                return;
            }

            TestExerciseUtility.doExercise(xmlParserService, doc);
        };
    }

}
