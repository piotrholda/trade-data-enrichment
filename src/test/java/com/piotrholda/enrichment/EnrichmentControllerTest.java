package com.piotrholda.enrichment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.reactive.function.BodyInserters;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@ExtendWith(OutputCaptureExtension.class)
class EnrichmentControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldCompleteMissingProductName(CapturedOutput output) {

        // given
        MultipartBodyBuilder multipartBodyBuilder = loadResourceAsMultipartBodyBuilder("trade.csv");
        String expectedResponseBody = loadResourceAsString("response.csv");

        // when-then
        webTestClient.post()
                .uri("/api/v1/enrich")
                .body(BodyInserters.fromMultipartData(multipartBodyBuilder.build()))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo(expectedResponseBody);

        assertThat(output).contains("Missing product name for product_id 11");
    }

    @Test
    void shouldSkipIncorrectDate(CapturedOutput output) {

        // given
        MultipartBodyBuilder multipartBodyBuilder = loadResourceAsMultipartBodyBuilder("incorrect_date/trade.csv");
        String expectedResponseBody = loadResourceAsString("incorrect_date/response.csv");

        // when-then
        webTestClient.post()
                .uri("/api/v1/enrich")
                .body(BodyInserters.fromMultipartData(multipartBodyBuilder.build()))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo(expectedResponseBody);

        assertThat(output).contains("Incorrect date format: incorrect date");
    }

    @Test
    void shouldReturnBadRequestForCorruptedFile() {

        // given
        MultipartBodyBuilder multipartBodyBuilder = loadResourceAsMultipartBodyBuilder("corrupted_file/trade.csv");

        // when-then
        webTestClient.post()
                .uri("/api/v1/enrich")
                .body(BodyInserters.fromMultipartData(multipartBodyBuilder.build()))
                .exchange()
                .expectStatus().isBadRequest();
    }

    private static MultipartBodyBuilder loadResourceAsMultipartBodyBuilder(String path) {
        MultipartBodyBuilder multipartBodyBuilder = new MultipartBodyBuilder();
        multipartBodyBuilder.part("file", new ClassPathResource(path))
                .contentType(MediaType.MULTIPART_FORM_DATA);
        return multipartBodyBuilder;
    }

    private static String loadResourceAsString(String path) {
        ClassPathResource resource = new ClassPathResource(path);
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            return FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}