package com.piotrholda.enrichment;

import com.piotrholda.enrichment.csv.CsvParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.stream.Stream;


@Service
@Slf4j
@RequiredArgsConstructor
class CsvProcessor {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String DATE = "date";
    private static final String PRODUCT_ID = "product_id";
    private static final String CURRENCY = "currency";
    private static final String PRICE = "price";
    private static final List<String> HEADER_NAMES = List.of(DATE, PRODUCT_ID, CURRENCY, PRICE);

    private final ProductProvider productProvider;

    Flux<String> processCsv(Reader reader) {
        Stream<Trade> trades = CsvParser.parse(reader, HEADER_NAMES,
                (csvRecord, headers) -> new Trade(csvRecord.get(headers.get(DATE)),
                        Long.parseLong(csvRecord.get(headers.get(PRODUCT_ID))),
                        csvRecord.get(headers.get(CURRENCY)),
                        new BigDecimal(csvRecord.get(headers.get(PRICE)))));
        return Flux.concat(
                Flux.just("date,product_name,currency,price"),
                Flux.fromStream(trades)
                        .filter(this::validateTrade)
                        .map(this::enrichTrade));
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
                trade.price().stripTrailingZeros().toPlainString();
    }
}
