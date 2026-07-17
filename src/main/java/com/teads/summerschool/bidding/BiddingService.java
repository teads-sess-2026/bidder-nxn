package com.teads.summerschool.bidding;
import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);
    private static final int MAX_CONCURRENT_BIDS = 200;

    private final AtomicInteger concurrentBids = new AtomicInteger(0);

    // Pre-indexed creative lookups for fast targeting-based filtering
    private volatile Map<String, List<Creative>> creativesByGeo = new ConcurrentHashMap<>();
    private volatile Map<String, List<Creative>> creativesByDevice = new ConcurrentHashMap<>();
    private volatile Map<String, List<Creative>> creativesBySegment = new ConcurrentHashMap<>();
    private volatile List<Creative> allCreativesCache = List.of();

    // Cached stats to avoid Redis reads on every bid
    private volatile long cachedWinCount = 0;
    private volatile long cachedLossCount = 0;
    private volatile double cachedAvgLossPrice = 0;
    private volatile long lastStatsFetch = 0;

    // Pacing: track spend over time to distribute evenly
    private final Instant startTime = Instant.now();
    private final AtomicLong totalSpentCents = new AtomicLong(0);

    // Budget snapshot: refreshed every 1 second instead of per-request Redis calls
    private volatile Map<String, Double> budgetSnapshot = new LinkedHashMap<>();
    private volatile long lastBudgetFetch = 0;

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
        buildCreativeIndex();
    }

    /**
     * Pre-index creatives by targeting dimensions (geo, device, segment) to avoid
     * filtering all 200 creatives on every bid. Refreshes every 5 seconds to pick up
     * any creative changes.
     */
    private void buildCreativeIndex() {
        creativeCache.getAll()
                .collectList()
                .subscribe(creatives -> {
                    allCreativesCache = creatives;

                    // Index by geo
                    Map<String, List<Creative>> byGeo = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String geo : extractTargetingValues(c.getAllowedGeos())) {
                            byGeo.computeIfAbsent(geo, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        // If no geo targeting, add to wildcard bucket
                        if (c.getAllowedGeos() == null || c.getAllowedGeos().isBlank()) {
                            byGeo.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesByGeo = byGeo;

                    // Index by device
                    Map<String, List<Creative>> byDevice = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String device : extractTargetingValues(c.getAllowedDevices())) {
                            byDevice.computeIfAbsent(device, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        if (c.getAllowedDevices() == null || c.getAllowedDevices().isBlank()) {
                            byDevice.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesByDevice = byDevice;

                    // Index by segment
                    Map<String, List<Creative>> bySegment = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String segment : extractTargetingValues(c.getAudienceSegments())) {
                            bySegment.computeIfAbsent(segment, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        if (c.getAudienceSegments() == null || c.getAudienceSegments().isBlank()) {
                            bySegment.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesBySegment = bySegment;

                    log.info("Creative index built: {} creatives, {} geos, {} devices, {} segments",
                            creatives.size(), byGeo.size(), byDevice.size(), bySegment.size());
                });

        // Refresh index every 5 seconds to pick up creative changes
        reactor.core.publisher.Flux.interval(Duration.ofSeconds(5))
                .flatMap(tick -> creativeCache.getAll().collectList())
                .subscribe(creatives -> {
                    allCreativesCache = creatives;

                    Map<String, List<Creative>> byGeo = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String geo : extractTargetingValues(c.getAllowedGeos())) {
                            byGeo.computeIfAbsent(geo, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        if (c.getAllowedGeos() == null || c.getAllowedGeos().isBlank()) {
                            byGeo.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesByGeo = byGeo;

                    Map<String, List<Creative>> byDevice = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String device : extractTargetingValues(c.getAllowedDevices())) {
                            byDevice.computeIfAbsent(device, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        if (c.getAllowedDevices() == null || c.getAllowedDevices().isBlank()) {
                            byDevice.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesByDevice = byDevice;

                    Map<String, List<Creative>> bySegment = new ConcurrentHashMap<>();
                    for (Creative c : creatives) {
                        for (String segment : extractTargetingValues(c.getAudienceSegments())) {
                            bySegment.computeIfAbsent(segment, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                        if (c.getAudienceSegments() == null || c.getAudienceSegments().isBlank()) {
                            bySegment.computeIfAbsent("*", k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(c);
                        }
                    }
                    creativesBySegment = bySegment;
                });
    }

    private Set<String> extractTargetingValues(String csv) {
        if (csv == null || csv.isBlank()) return Set.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * getRemainingBudget() does a DB query plus one Redis call per creative — under
     * DB/Redis pool contention (e.g. remote backing services with WAN latency) it can
     * queue for a connection indefinitely. /actuator/prometheus has no timeout of its
     * own, so an unbounded gauge supplier here stalls the entire scrape response.
     * Bound it the same way /api/bid bounds biddingService.bid(), and fall back to the
     * last known value instead of blocking Prometheus forever.
     *
     * <p>Micrometer's Gauge contract takes a plain synchronous Supplier<Number>, polled by the
     * Prometheus scrape thread — there's no reactive variant, so this is the one sanctioned
     * .block() outside of startup/Kafka-listener boundaries elsewhere in this codebase.
     */
    private double getRemainingBudgetSafe() {
        try {
            Double value = getRemainingBudget()
                    .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                    .onErrorReturn(lastKnownBudget)
                    .block();
            lastKnownBudget = value;
            return value;
        } catch (Exception ex) {
            return lastKnownBudget;
        }
    }

    public Mono<Optional<BidResponse>> bid(BidRequest request) {
        // Rate limit: reject if too many concurrent bids already processing
        if (concurrentBids.get() >= MAX_CONCURRENT_BIDS) {
            metrics.recordNoBid("rate_limited");
            return Mono.just(Optional.empty());
        }

        // Pacing: only bid if we're behind or on schedule for spending
        if (!shouldBid()) {
            metrics.recordNoBid("rate_limited");
            return Mono.just(Optional.empty());
        }

        long start = System.nanoTime();
        metrics.summerschool_bids.increment();
        metrics.recordRequest();

        concurrentBids.incrementAndGet();

        return Mono.fromCallable(() -> {
                    // Use pre-indexed creatives instead of fetching all 200
                    List<Creative> candidates = getTargetedCreatives(
                            request.targeting().geo(),
                            request.targeting().deviceType(),
                            request.targeting().audienceSegment()
                    );

                    if (candidates.isEmpty()) {
                        metrics.recordNoBid("no_targeted_creatives");
                        return Optional.<BidResponse>empty();
                    }

                    // Filter by floor price and budget
                    Map<String, Double> budgets = getBudgetSnapshot();
                    List<Creative> eligibleCreatives = candidates.stream()
                            .filter(c -> c.isWithinMaxBid(request.floorPrice()))
                            .filter(c -> budgets.getOrDefault(c.getId(), 25.0) > 5.0)
                            .toList();

                    if (eligibleCreatives.isEmpty()) {
                        metrics.recordNoBid("no_eligible_creatives");
                        return Optional.<BidResponse>empty();
                    }

                    // Select creative and compute bid price
                    Creative selectedCreative = eligibleCreatives.get(
                            ThreadLocalRandom.current().nextInt(eligibleCreatives.size())
                    );
                    double bidPrice = computeBidPrice(request);

                    metrics.recordBid();
                    metrics.recordLatency((int) ((System.nanoTime() - start) / 1_000_000));

                    // Record asynchronously (fire-and-forget)
                    ownBidCache.record(request.requestId(), selectedCreative.getId(), bidPrice);

                    BidResponse response = new BidResponse(
                            request.requestId(),
                            bidPrice,
                            toCreativeDto(selectedCreative)
                    );

                    return Optional.of(response);
                })
                .doFinally(signal -> concurrentBids.decrementAndGet());
    }

    /**
     * Get creatives matching the request's targeting using pre-built indexes.
     * Returns intersection of geo, device, and segment matches (or wildcards).
     */
    private List<Creative> getTargetedCreatives(String geo, String device, String segment) {
        // Get candidates from geo index (or wildcard if geo not found)
        List<Creative> geoCandidates = creativesByGeo.getOrDefault(geo,
                creativesByGeo.getOrDefault("*", List.of()));

        // Get candidates from device index
        List<Creative> deviceCandidates = creativesByDevice.getOrDefault(device,
                creativesByDevice.getOrDefault("*", List.of()));

        // Get candidates from segment index
        List<Creative> segmentCandidates = creativesBySegment.getOrDefault(segment,
                creativesBySegment.getOrDefault("*", List.of()));

        // Return intersection of all three targeting dimensions
        Set<String> geoIds = geoCandidates.stream().map(Creative::getId).collect(Collectors.toSet());
        Set<String> deviceIds = deviceCandidates.stream().map(Creative::getId).collect(Collectors.toSet());
        Set<String> segmentIds = segmentCandidates.stream().map(Creative::getId).collect(Collectors.toSet());

        geoIds.retainAll(deviceIds);
        geoIds.retainAll(segmentIds);

        // Return creatives that match all targeting criteria
        return allCreativesCache.stream()
                .filter(c -> geoIds.contains(c.getId()))
                .toList();
    }

    private boolean shouldBid() {
        long elapsedSeconds = Duration.between(startTime, Instant.now()).getSeconds() + 1;
        long durationSeconds = properties.getCompetition().getDurationSeconds();
        if (durationSeconds <= 0) durationSeconds = 600;

        double totalBudget = properties.getCreativeBudget() * 200.0; // $25 × 200 = $5000
        double expectedSpend = (double) elapsedSeconds / durationSeconds * totalBudget;
        double actualSpend = totalSpentCents.get() / 100.0;

        // Allow up to 3x ahead of linear schedule — competition is a race, not a marathon
        return actualSpend < expectedSpend * 3;
    }

    private Map<String, Double> getBudgetSnapshot() {
        long now = System.currentTimeMillis();
        if (now - lastBudgetFetch > 1000) {
            lastBudgetFetch = now;
            try {
                Map<String, Double> fresh = getRemainingBudgets().block(Duration.ofMillis(100));
                if (fresh != null) {
                    budgetSnapshot = fresh;
                }
            } catch (Exception e) {
                // use stale snapshot on timeout
            }
        }
        return budgetSnapshot;
    }

    public void recordSpend(double amount) {
        totalSpentCents.addAndGet((long) (amount * 100));
    }

    private double computeBidPrice(BidRequest request) {
        double floorPrice = request.floorPrice();

        // Refresh cached stats every 500ms instead of querying Redis on every bid
        refreshStatsCache();

        long winCount = cachedWinCount;
        long lossCount = cachedLossCount;
        long totalBids = winCount + lossCount;

        // Cold start: bid aggressively to collect data
        if (totalBids < 20) {
            return floorPrice * 1.10;
        }

        double winRate = (double) winCount / totalBids;

        double targetBid;
        if (winRate > 0.6) {
            // Winning a lot: save some budget
            targetBid = floorPrice * 1.05;
        } else if (winRate > 0.3) {
            // Healthy range: bid moderately above floor
            targetBid = floorPrice * 1.10;
        } else {
            // Losing too much: match the market
            double avgLoss = cachedAvgLossPrice;
            targetBid = avgLoss > 0 ? avgLoss * 1.02 : floorPrice * 1.15;
        }

        return Math.max(targetBid, floorPrice * 1.05);
    }

    /**
     * Refresh stats cache every 500ms to avoid Redis reads on every bid.
     */
    private void refreshStatsCache() {
        long now = System.currentTimeMillis();
        if (now - lastStatsFetch > 500) {
            lastStatsFetch = now;
            cachedWinCount = statsCache.getWinCount();
            cachedLossCount = statsCache.getLossCount();
            cachedAvgLossPrice = statsCache.getRollingAverageLossPrice();
        }
    }

    /** Total remaining budget across all this bidder's creatives. */
    public Mono<Double> getRemainingBudget() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()))
                .reduce(0.0, Double::sum);
    }

    /** Remaining budget per creative id. */
    public Mono<Map<String, Double>> getRemainingBudgets() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()).map(budget -> Map.entry(c.getId(), budget)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new);
    }

    private CreativeDto toCreativeDto(Creative creative) {
        return new CreativeDto(
                creative.getId(),
                creative.getName(),
                creative.getDescription(),
                creative.getImageUrl(),
                creative.getCallToAction(),
                splitCsv(creative.getAllowedGeos()),
                splitCsv(creative.getAllowedDevices()),
                splitCsv(creative.getAudienceSegments())
        );
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

}
