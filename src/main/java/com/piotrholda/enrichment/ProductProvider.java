package com.piotrholda.enrichment;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

public interface ProductProvider {

    String getProductName(Long key);

}
