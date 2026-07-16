package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);
    private static final int MAX_CONCURRENT_BIDS = 80;

    private final Random random = new Random();
    private final java.util.concurrent.atomic.AtomicInteger concurrentBids = new java.util.concurrent.atomic.AtomicInteger(0);

    // Pacing: track spend over time to distribute evenly
    private final Instant startTime = Instant.now();
    private final AtomicLong totalSpentCents = new AtomicLong(0);

    // Budget snapshot: refreshed every 1 second instead of per-request Redis calls
    private volatile Map<String, Double> budgetSnapshot = new LinkedHashMap<>();
    private volatile long lastBudgetFetch = 0;

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidRecordRepository bidRecordRepository;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
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

        return creativeCache.getAll()
                .filter(c -> c.isWithinMaxBid(request.floorPrice()))
                .collectList()
                .flatMap(withinBudget -> {
                    if (withinBudget.isEmpty()) {
                        metrics.recordNoBid("floor_exceeds_max_bid");
                        return Mono.just(Optional.<BidResponse>empty());
                    }

                    // Use in-memory budget snapshot instead of Redis calls per creative
                    Map<String, Double> budgets = getBudgetSnapshot();
                    List<Creative> eligibleCreatives = withinBudget.stream()
                            .filter(c -> c.matches(
                                    request.targeting().geo(),
                                    request.targeting().deviceType(),
                                    request.targeting().audienceSegment()))
                            .filter(c -> budgets.getOrDefault(c.getId(), 25.0) > 5.0)
                            .toList();

                    if (eligibleCreatives.isEmpty()) {
                        metrics.recordNoBid("no_eligible_creatives");
                        return Mono.just(Optional.<BidResponse>empty());
                    }

                    Creative selectedCreative = eligibleCreatives.get(random.nextInt(eligibleCreatives.size()));
                    double bidPrice = computeBidPrice(request);

                    metrics.recordBid();
                    metrics.recordLatency((int) ((System.nanoTime() - start) / 1_000_000));

                    ownBidCache.record(request.requestId(), selectedCreative.getId(), bidPrice);

                    BidResponse response = new BidResponse(
                            request.requestId(),
                            bidPrice,
                            toCreativeDto(selectedCreative)
                    );

                    return Mono.just(Optional.of(response));
                })
                .doFinally(signal -> concurrentBids.decrementAndGet());
    }

    private boolean shouldBid() {
        long elapsedSeconds = Duration.between(startTime, Instant.now()).getSeconds() + 1;
        long durationSeconds = properties.getCompetition().getDurationSeconds();
        if (durationSeconds <= 0) durationSeconds = 1800;

        double totalBudget = properties.getCreativeBudget() * 200.0; // $25 × 200 = $5000
        double expectedSpend = (double) elapsedSeconds / durationSeconds * totalBudget;
        double actualSpend = totalSpentCents.get() / 100.0;

        return actualSpend < expectedSpend * 1.1;
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

        long winCount = statsCache.getWinCount();
        long lossCount = statsCache.getLossCount();
        long totalBids = winCount + lossCount;

        // Cold start: bid just above floor
        if (totalBids < 10) {
            return floorPrice * 1.02;
        }

        double winRate = (double) winCount / totalBids;

        double targetBid;
        if (winRate > 0.5) {
            // Winning too much: bid minimum to save budget
            targetBid = floorPrice * 1.01;
        } else if (winRate > 0.2) {
            // Good range: bid slightly above floor
            targetBid = floorPrice * 1.03;
        } else {
            // Losing too much: bid closer to market clearing price
            double avgLoss = statsCache.getRollingAverageLossPrice();
            targetBid = avgLoss > 0 ? avgLoss * 0.95 : floorPrice * 1.05;
        }

        return Math.max(targetBid, floorPrice * 1.01);
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

    private Flux<Creative> matchingCreatives(BidRequest request, Flux<Creative> all) {
        return all.filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()));
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

    private BidRecord buildRecord(BidRequest request) {
        BidRecord record = new BidRecord();
        record.setRequestId(request.requestId());
        record.setFloorPrice(request.floorPrice());
        if (request.targeting() != null) {
            record.setGeo(request.targeting().geo());
            record.setDeviceType(request.targeting().deviceType());
            record.setAudienceSegment(request.targeting().audienceSegment());
        }
        return record;
    }
}
