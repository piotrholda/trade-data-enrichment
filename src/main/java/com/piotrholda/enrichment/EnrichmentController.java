package com.piotrholda.enrichment;

import lombok.RequiredArgsConstructor;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class EnrichmentController {

    private final CsvProcessor processor;

    @PostMapping(value = "/enrich", consumes = "multipart/form-data", produces = "text/csv")
    public Flux<String> enrich(@RequestPart("file") FilePart filePart) {
        return filePart.content()
                .map(dataBuffer -> new InputStreamReader(dataBuffer.asInputStream(), StandardCharsets.UTF_8))
                .flatMap(processor::processCsv);
    }
}
