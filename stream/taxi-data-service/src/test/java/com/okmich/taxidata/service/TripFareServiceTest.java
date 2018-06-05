/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.taxidata.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author datadev
 */
public class TripFareServiceTest {

    @Test
    public void testGoodCase() {
        String payload = "2013008681,2013007085,VTS,1,,2013-12-01 00:00:00,2013-12-01 00:06:00,6,360,1.24,-73.983803,40.743511,-73.990311,40.729755";
        JsonNode tripNode = RawTripDataProcessingService.transform(payload);

        System.out.println(tripNode.toString());

        String payload2 = "2013008681,2013007085,VTS,2013-12-01 00:00:00,CSH,6.5,0.5,0.5,0,0,7.5";
        JsonNode fareNnode = RawFareDataProcessingService.transform(payload2);

        System.out.println(fareNnode.toString());

        JsonNode mergedNode = TripFareService.transform(tripNode, fareNnode);

        String medallion = mergedNode.get("medallion").asText();
        Assert.assertEquals("2013008681", medallion);

        System.out.println(mergedNode.toString());
    }

}
