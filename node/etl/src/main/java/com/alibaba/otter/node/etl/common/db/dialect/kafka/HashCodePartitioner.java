package com.alibaba.otter.node.etl.common.db.dialect.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class HashCodePartitioner implements Partitioner {

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
		String k = (String) key;
		partition = Math.abs(k.hashCode()) % numPartitions;
		return partition;
	}
	
	public HashCodePartitioner(VerifiableProperties props) {
	}

	

}
