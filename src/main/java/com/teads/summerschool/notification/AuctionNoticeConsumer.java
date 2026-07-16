package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final WinNoticeRepository winNoticeRepository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        String requestId = "unknown";
        try {
            log.debug("KAFKA  Parsing protobuf message...");
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);
            requestId = notice.getRequestId();

            log.debug("KAFKA  Parsed: id={} winner={} clearing={} creative={}",
                    notice.getRequestId(), notice.getWinningBidderId(),
                    notice.getClearingPrice(), notice.getCreativeId());

            // This topic broadcasts EVERY auction's outcome to EVERY bidder, so most
            // messages a bidder receives are ones it never bid on. Filter on the
            // in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
            // Postgres — an O(1) local lookup instead of a DB round trip on every message.
            log.debug("KAFKA  Checking OwnBidCache for id={}", requestId);
            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());
            if (ourBid == null) {
                log.debug("KAFKA  Not our bid, skipping id={}", requestId);
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());

            log.debug("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                log.debug("KAFKA  Processing WIN for id={}", requestId);
                double clearingPrice = notice.getClearingPrice();
                log.debug("KAFKA  Clearing price: {}", clearingPrice);

                log.debug("KAFKA  Recording win in statsCache for creative={}", ourBid.creativeId());
                statsCache.recordWin(ourBid.creativeId(), clearingPrice).block();
                log.debug("KAFKA  Win recorded in statsCache");

                log.debug("KAFKA  Creating WinNotice record");
                WinNotice winNotice = new WinNotice(
                        notice.getRequestId(),
                        properties.getId(),
                        clearingPrice,
                        ourBid.bidPrice()
                );

                log.debug("KAFKA  Saving WinNotice to repository");
                winNoticeRepository.save(winNotice).block();
                log.debug("KAFKA  WinNotice saved");

                log.debug("KAFKA  Recording win metric");
                metrics.recordWin(clearingPrice);

                log.info("** WIN  id={} creative={} clearing={} bid={} overpaid={}",
                        notice.getRequestId(), ourBid.creativeId(), clearingPrice,
                        ourBid.bidPrice(), ourBid.bidPrice() - clearingPrice);
            } else {
                log.debug("KAFKA  Processing LOSS for id={}", requestId);
                metrics.recordLoss();

                log.debug("** LOSS  id={} bid={} clearing={} gap={}",
                        notice.getRequestId(), ourBid.bidPrice(), notice.getClearingPrice(),
                        notice.getClearingPrice() - ourBid.bidPrice());
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  id={} failed at: {} - {}",
                    requestId, e.getClass().getSimpleName(), e.getMessage(), e);
        }
    }
}