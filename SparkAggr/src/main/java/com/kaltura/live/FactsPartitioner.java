package com.kaltura.live;

import java.util.Random;

import org.apache.spark.Partitioner;

public class FactsPartitioner extends Partitioner {

	static Random randomGenerator = new Random();
	@Override
	public int getPartition(Object arg0) {
		return arg0.toString().hashCode() * randomGenerator.nextInt(10) / numPartitions();
		
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return 12;
	}

}
