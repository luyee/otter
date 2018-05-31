package com.alibaba.otter.node.etl.select.selector.kafka;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The partition offset are stored in an external storage. In this case in a
 * file system.
 * 将分区的offset信息保存再外部存储中，当前offset管理器，存储的是文件系统。
 * <p/>
 */
public class OffsetManager {
	private String storagePrefix;
	
	public OffsetManager(String storagePrefix) {
		this.storagePrefix = storagePrefix;
	}

	/**
	 * Overwrite the offset for the topic in an external storage.
	 * 保存offset，注意会重写offset
	 * @param topic Topic name.
	 * @param partition  Partition of the topic.
	 * @param offset offset to be stored.
	 */
	public void saveOffsetInExternalStore(String topic, int partition, long offset) {
		try {
			FileWriter writer = new FileWriter(storageName(topic, partition), false);//append模式为false，重写文件内容
			BufferedWriter bufferedWriter = new BufferedWriter(writer);
			bufferedWriter.write(offset + "");
			bufferedWriter.flush();
			bufferedWriter.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	/**
	 * 从offset存储文件中，读取topic对应分区的offset
	 * @param topic
	 * @param partition
	 * @return the last offset + 1 for the provided topic and partition.
	 */
	@SuppressWarnings({ "resource" })
	public long readOffsetFromExternalStore(String topic, int partition) {
		try {
			Stream<String> stream = Files.lines(Paths.get(storageName(topic, partition)));
			return Long.parseLong(stream.collect(Collectors.toList()).get(0)) + 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * topic分区offset信息存储位置
	 * @param topic
	 * @param partition
	 * @return
	 */
	private String storageName(String topic, int partition) {
		return "../position/"+storagePrefix + "_" + topic + "_" + partition;
	}

}
