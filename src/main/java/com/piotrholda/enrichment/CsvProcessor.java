package com.piotrholda.enrichment;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.Reader;

@Service
class CsvProcessor {

    Flux<String> processCsv(Reader reader) {
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
    }

    private String processRow(CSVRecord csvRecord) {
        return System.lineSeparator() + String.join(",", csvRecord.toList());
    }
}
