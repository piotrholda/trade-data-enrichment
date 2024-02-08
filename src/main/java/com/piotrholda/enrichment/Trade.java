package com.piotrholda.enrichment;

import java.math.BigDecimal;

record Trade(String date, long productId, String currency, BigDecimal price) {
}
