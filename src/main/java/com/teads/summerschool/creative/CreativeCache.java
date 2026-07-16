package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Lookup for this bidder's creative catalog with short-TTL in-memory cache.
 * Creatives change infrequently but are read on every bid, so we cache for 2 seconds
 * to avoid hitting Postgres 40+ times per second under concurrent load.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);
    private static final long CACHE_TTL_MS = 2000; // 2 second cache

    private final CreativeRepository repository;
    private final BidderProperties properties;

    private volatile List<Creative> cachedCreatives = null;
    private volatile long lastFetchTime = 0;

    public CreativeCache(CreativeRepository repository, BidderProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public Flux<Creative> getAll() {
        long now = System.currentTimeMillis();

        // Return cached creatives if fresh
        if (cachedCreatives != null && (now - lastFetchTime) < CACHE_TTL_MS) {
            return Flux.fromIterable(cachedCreatives);
        }

        // Cache miss or stale: fetch from DB and update cache
        return repository.findByBidderId(properties.getId())
                .collectList()
                .doOnNext(creatives -> {
                    cachedCreatives = creatives;
                    lastFetchTime = System.currentTimeMillis();
                })
                .flatMapMany(Flux::fromIterable);
    }

    /** Force cache refresh and log catalog size. Used by CreativeSeeder after seeding. */
    public Mono<Void> refresh() {
        // Invalidate cache to force fresh read
        cachedCreatives = null;
        lastFetchTime = 0;

        return getAll().count()
                .doOnNext(n -> log.info("Creative catalog seeded: {} creatives", n))
                .then();
    }
}
