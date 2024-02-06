package com.piotrholda.enrichment.product;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

//TODO For very large product files needs to be implemented with the external storage. E.g. database.
@Service
class InMemoryProductRepository implements ProductRepository {

    private Map<Long, Product> products = new ConcurrentHashMap<>();

    @Override
    public void save(Product product) {
        products.put(product.key(), product);
    }

    @Override
    public Optional<Product> getProduct(Long key) {
        return Optional.ofNullable(products.get(key));
    }
}
