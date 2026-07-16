package com.teads.summerschool.bidding.dto;

public record BidRequest(
        String requestId, //will be used to track our bud, when we win/lose kafka uses the same id
        //this id is stored OwnBidCache to match wins back to the bid
        double floorPrice,//lowest price u have to pay to participate
        Targeting targeting //information about the user/context
) {}
