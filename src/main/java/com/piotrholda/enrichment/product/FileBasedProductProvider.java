package com.piotrholda.enrichment.product;

import com.piotrholda.enrichment.ProductProvider;
import com.piotrholda.enrichment.csv.HeaderParser;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.piotrholda.enrichment.csv.HeaderParser.getHeaderIndex;

@Service
@Slf4j
@RequiredArgsConstructor
class FileBasedProductProvider implements ProductProvider {

    private static final String DEFAULT_PRODUCT_NAME = "Missing Product Name";

    private final ProductRepository productRepository;

    @PostConstruct
    public void loadProductData() {
        String fileName = "src/test/resources/product.csv";
        try (Reader reader = Files.newBufferedReader(Paths.get(fileName))) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();
            try (CSVParser parser = csvFormat.parse(reader)) {
                Map<String, Integer> headerMap = parser.getHeaderMap();
                final int keyIndex = getHeaderIndex(headerMap, "product_id");
                final int nameIndex = getHeaderIndex(headerMap, "product_name");
                parser.stream()
                        .map(csvRecord -> new Product(Long.parseLong(csvRecord.get(keyIndex)), csvRecord.get(nameIndex)))
                        .forEach(productRepository::save);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load product file %s".formatted(fileName), e);
        }
    }

    @Override
    public String getProductName(Long key) {
        return getProductNameIfExists(key)
                .orElseGet(() -> {
                    log.error("Missing product name for product_id %d".formatted(key));
                    return DEFAULT_PRODUCT_NAME;
                });
    }

    protected Optional<String> getProductNameIfExists(Long key) {
        return productRepository.getProduct(key)
                .map(Product::name);
    }
}
