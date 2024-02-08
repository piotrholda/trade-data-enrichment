package com.piotrholda.enrichment.csv;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HeaderParser {

    public static int getHeaderIndex(Map<String, Integer> headerMap, String headerName) {
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
