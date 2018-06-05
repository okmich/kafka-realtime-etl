package com.okmich.taxidata.service;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class RawFareDataProcessingServiceTest {

	@Test
	public void testGoodCase() {
		String payload = "2013008681,2013007085,VTS,2013-12-01 00:00:00,CSH,6.5,0.5,0.5,0,0,7.5";
		JsonNode node = RawFareDataProcessingService.transform(payload);

		String medallion = node.get("medallion").asText();
		Assert.assertEquals(medallion, "2013008681");

		System.out.println(node.toString());
	}

	@Test
	public void testBaseCase() {
		String payload = "medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount";
		JsonNode node = RawTripDataProcessingService.transform(payload);

		Assert.assertNull(node);
	}

}
