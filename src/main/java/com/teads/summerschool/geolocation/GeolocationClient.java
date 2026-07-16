package com.teads.summerschool.geolocation;

import com.teads.summerschool.geolocation.dto.GeolocationResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

//@Service
public class GeolocationClient {

    private final WebClient webClient;
    private final GeolocationProperties properties;

    public GeolocationClient(WebClient.Builder webClientBuilder, GeolocationProperties properties) {
        this.properties = properties;
        this.webClient = webClientBuilder
                .baseUrl(properties.getBaseUrl())
                .build();
    }

    public Mono<GeolocationResponse> getGeolocation(String ipAddress) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/ipgeo")
                        .queryParam("apiKey", properties.getApiKey())
                        .queryParam("ip", ipAddress)
                        .build())
                .retrieve()
                .bodyToMono(GeolocationResponse.class)
                .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                .onErrorResume(e -> {
                    return Mono.empty();
                });
    }

    public Mono<GeolocationResponse> getCurrentIpGeolocation() {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/ipgeo")
                        .queryParam("apiKey", properties.getApiKey())
                        .build())
                .retrieve()
                .bodyToMono(GeolocationResponse.class)
                .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                .onErrorResume(e -> {
                    return Mono.empty();
                });
    }
}
