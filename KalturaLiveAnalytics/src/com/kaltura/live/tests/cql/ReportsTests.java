package com.kaltura.live.tests.cql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ EntryGeoTimeLineTest.class, 
	EntrySyndicationTotalTest.class,
	EntryTimeLineTest.class, 
	EntryTotalLiveTest.class,
	EntryTotalPastTest.class, 
	PartnerTotalLiveTest.class,
	PartnerTotalPastTest.class })
public class ReportsTests {

}
