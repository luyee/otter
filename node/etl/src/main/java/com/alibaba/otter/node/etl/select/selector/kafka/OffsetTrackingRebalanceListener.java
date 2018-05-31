package com.alibaba.otter.node.etl.select.selector.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class OffsetTrackingRebalanceListener implements ConsumerRebalanceListener {
	private OffsetManager offsetManager;
	private Consumer<String, String> consumer;
	public OffsetTrackingRebalanceListener(Consumer<String, String> consumer,OffsetManager offsetManager) {
		this.consumer = consumer;
		this.offsetManager = offsetManager;
	}
    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store
     * on the start of a rebalance operation. This method will be called before a rebalance operation starts
     * and after the consumer stops fetching data. It is recommended that offsets should be committed in this
     * callback to either Kafka or a custom offset store to prevent duplicate data. 
     * 保存主题分区的offset信息
     */
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(),
					consumer.position(partition));
		}
	}
	/**
	 * A callback method the user can implement to provide handling of customized offsets on completion 
	 * of a successful partition re-assignment. 
	 * This method will be called after an offset re-assignment completes and before the consumer starts fetching data.
	 * It is guaranteed that all the processes in a consumer group will execute their onPartitionsRevoked(Collection) 
	 * callback before any instance executes its onPartitionsAssigned(Collection) callback.
	 * 定位主题各分区的offset位置
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			consumer.seek(partition,
					offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
		}
	}
}
