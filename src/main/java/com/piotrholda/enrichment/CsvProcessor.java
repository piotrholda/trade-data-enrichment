package com.piotrholda.enrichment;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

import static com.piotrholda.enrichment.csv.HeaderParser.getHeaderIndex;

@Service
@Slf4j
@RequiredArgsConstructor
class CsvProcessor {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final ProductProvider productProvider;

    Flux<String> processCsv(Reader reader) {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
        try (CSVParser parser = csvFormat.parse(reader)) {
            Map<String, Integer> headerMap = parser.getHeaderMap();
            final int dateIndex = getHeaderIndex(headerMap, "date");
            final int productIdIndex = getHeaderIndex(headerMap, "product_id");
            final int currencyIndex = getHeaderIndex(headerMap, "currency");
            final int priceIndex = getHeaderIndex(headerMap, "price");
            return Flux.concat(
                    Flux.just("date,product_name,currency,price"),
                    Flux.fromIterable(parser.getRecords())
                            .map(csvRecord -> new Trade(csvRecord.get(dateIndex),
                                    Long.parseLong(csvRecord.get(productIdIndex)),
                                    csvRecord.get(currencyIndex),
                                    new BigDecimal(csvRecord.get(priceIndex))))
                            .filter(this::validateTrade)
                            .map(this::enrichTrade));
        } catch (Exception e) {
            return Flux.error(e);
        }
    }

    private boolean validateTrade(Trade trade) {
        try {
            LocalDate.parse(trade.date(), DATE_FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            log.error("Incorrect date format: %s".formatted(trade.date()));
            return false;
        }
    }

    private String enrichTrade(Trade trade) {
        String productName = productProvider.getProductName(trade.productId());
        return System.lineSeparator() +
                trade.date() + "," +
                productName + "," +
                trade.currency() + "," +
                trade.price().stripTrailingZeros().toEngineeringString();
    }
}
