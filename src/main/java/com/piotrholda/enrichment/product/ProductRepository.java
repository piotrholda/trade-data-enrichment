package com.piotrholda.enrichment.product;

import java.util.Optional;

interface ProductRepository {
    void save(Product product);
    Optional<Product> getProduct(Long key);
}
