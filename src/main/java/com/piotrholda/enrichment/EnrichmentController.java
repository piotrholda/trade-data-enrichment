package com.piotrholda.enrichment;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/v1")
public class EnrichmentController {

    @PostMapping(value = "/enrich", consumes = "multipart/form-data", produces = "text/csv")
    public Flux<String> enrich(@RequestPart("file") FilePart filePart) {
        return filePart.content()
                .map(dataBuffer -> {
                    InputStream inputStream = dataBuffer.asInputStream();
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                    return new BufferedReader(inputStreamReader);
                })
                .flatMap(reader -> {
                    CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .build();
                    try (CSVParser parser = csvFormat.parse(reader)) {
                        return Flux.concat(
                                Flux.just(String.join(",", parser.getHeaderNames())),
                                Flux.fromIterable(parser.getRecords())
                                        .map(this::processRow));
                    } catch (Exception e) {
                        return Flux.error(e);
                    }
                });
    }

    private String processRow(CSVRecord csvRecord) {
        return System.lineSeparator() + String.join(",", csvRecord.toList());
    }
}
