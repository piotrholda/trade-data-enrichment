package com.piotrholda.enrichment.csv;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvParser {

    public static <T> Stream<T> parse(Reader reader, List<String> headerNames, BiFunction<CSVRecord, Map<String, Integer>, T> recordMapper) {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
        try (CSVParser parser = csvFormat.parse(reader)) {
            Map<String, Integer> headers = parseHeaders(parser, headerNames);
            return parser.getRecords().stream()
                    .map(csvRecord -> recordMapper.apply(csvRecord, headers));
        } catch (IOException e) {
            throw new UncheckedIOException("Error parsing CSV", e);
        }
    }

    private static Map<String, Integer> parseHeaders(CSVParser parser, List<String> headerNames) {
        Map<String, Integer> headerMap = parser.getHeaderMap();
        return headerNames.stream()
                .collect(Collectors.toMap(headerName -> headerName, headerName -> getHeaderIndex(headerMap, headerName)));
    }

    private static int getHeaderIndex(Map<String, Integer> headerMap, String headerName) {
        for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            String name = entry.getKey();
            int index = entry.getValue();
            if (name.trim().equals(headerName)) {
                return index;
            }
        }
        throw new IllegalArgumentException("Header %s not found in product file".formatted(headerName));
    }
}
