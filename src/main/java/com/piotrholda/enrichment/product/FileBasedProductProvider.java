package com.piotrholda.enrichment.product;

import com.piotrholda.enrichment.ProductProvider;
import com.piotrholda.enrichment.csv.CsvParser;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
class FileBasedProductProvider implements ProductProvider {

    private static final String DEFAULT_PRODUCT_NAME = "Missing Product Name";
    public static final String PRODUCT_ID = "product_id";
    public static final String PRODUCT_NAME = "product_name";
    public static final List<String> HEADER_NAMES = List.of(PRODUCT_ID, PRODUCT_NAME);

    private final ProductRepository productRepository;

    @Value("${product.file}")
    private String productFile;

    @PostConstruct
    private void loadProductData() {
        try (Reader reader = Files.newBufferedReader(Paths.get(productFile))) {
            Stream<Product> products = CsvParser.parse(reader, HEADER_NAMES,
                    (csvRecord, headers) ->
                            new Product(Long.parseLong(
                                    csvRecord.get(headers.get(PRODUCT_ID))),
                                    csvRecord.get(headers.get(PRODUCT_NAME))));
            products.forEach(productRepository::save);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to load product file %s".formatted(productFile), e);
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

    private Optional<String> getProductNameIfExists(Long key) {
        return productRepository.getProduct(key)
                .map(Product::name);
    }
}
