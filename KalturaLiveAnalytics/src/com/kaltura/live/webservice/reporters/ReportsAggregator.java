package com.kaltura.live.webservice.reporters;

import com.kaltura.live.webservice.model.LiveStats;

public class ReportsAggregator {
	
	// Sum based
	protected long plays = 0;
	protected long audience = 0;
	protected long secondsViewed = 0;
	
	// Average per minute
	protected long bufferTime = 0;
	protected long bitRate = 0;
	protected long bitrateCnt = 0;
	
	public void aggregateResult(long plays, long audience, long bufferTime, long bitRate, long bitrateCount) {
		this.plays += plays;
		this.audience += audience;
		this.secondsViewed += audience * 10;
		
		this.bufferTime += bufferTime;
		this.bitRate += bitRate;
		this.bitrateCnt += bitrateCount;
	}
	
	public void fillObject(LiveStats stats) {
		stats.setPlays(plays);
		stats.setAudience(audience);
		stats.setSecondsViewed(secondsViewed);
		
		stats.setBufferTime(calcAveragePerMinute(bufferTime, plays + audience, 6));
		stats.setAvgBitrate(calcAveragePerMinute(bitRate, bitrateCnt, 1));
	}
	
	
	protected static float calcAveragePerMinute(long parts, long denominator, int normFactor) {
		if(denominator > 0)
			// Round to 2 decimal points
			// And set it to be average on one minute
			return (float) (Math.round((normFactor * 100.0 * parts) / denominator) / 100.0);
		return 0;
	}
	
}
