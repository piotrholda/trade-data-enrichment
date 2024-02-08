package com.piotrholda.enrichment.product;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

//TODO For very large product files needs to be implemented as the external storage. E.g. database.
@Service
class InMemoryProductRepository implements ProductRepository {

    private Map<Long, Product> products = new HashMap<>();

    @Override
    public void save(Product product) {
        products.put(product.key(), product);
    }

    @Override
    public Optional<Product> getProduct(Long key) {
        return Optional.ofNullable(products.get(key));
    }
}
