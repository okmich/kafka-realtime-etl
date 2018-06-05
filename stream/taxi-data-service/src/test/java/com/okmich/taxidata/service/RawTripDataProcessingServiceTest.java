package com.okmich.taxidata.service;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class RawTripDataProcessingServiceTest {

	@Test
	public void testGoodCase() {
		String payload = "2013008681,2013007085,VTS,1,,2013-12-01 00:00:00,2013-12-01 00:06:00,6,360,1.24,-73.983803,40.743511,-73.990311,40.729755";
		JsonNode node = RawTripDataProcessingService.transform(payload);
	
		String medallion = node.get("medallion").asText();
		Assert.assertEquals("2013008681", medallion);
		
		System.out.println(node.toString());
	}

	@Test
	public void testBaseCase() {
		String payload = "medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude";
		JsonNode node = RawTripDataProcessingService.transform(payload);

		Assert.assertNull(node);
	}

}
