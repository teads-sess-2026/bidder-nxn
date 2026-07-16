package com.teads.summerschool.bidding.dto;

public record BidResponse(
        String requestId,
        double bidPrice, //the price that won
        CreativeDto creative
) {}
