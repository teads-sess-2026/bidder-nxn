package com.teads.summerschool.geolocation;

import com.teads.summerschool.geolocation.dto.GeolocationResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/geolocation")
public class GeolocationController {

    private final GeolocationClient geolocationClient;

    public GeolocationController(GeolocationClient geolocationClient) {
        this.geolocationClient = geolocationClient;
    }

    @GetMapping
    public Mono<GeolocationResponse> getGeolocation(@RequestParam(required = false) String ip) {
        if (ip != null && !ip.isEmpty()) {
            return geolocationClient.getGeolocation(ip);
        }
        return geolocationClient.getCurrentIpGeolocation();
    }
}
