package com.alibaba.otter.shared.common.model.config.data.kafka;

import java.util.Map;
import java.util.Properties;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
/**
 * kafka 连接参数
 * @author zhangxhui joezxh@qq.com
 * 
 * date: 2017-02-10
 */
public class KafkaMediaSource extends DataMediaSource {

	private static final long serialVersionUID = 1966567625036110432L;

	private String bootstrapServers;
	private String zookeeperConnect;
	  private Properties        properties;
	private long batchSize=128000L;
	private long bufferMemory=97108864L;
	
	private String url ;
	
	

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getBootstrapServers() {
		 return url;
		//return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(long batchSize) {
		this.batchSize = batchSize;
	}

	public long getBufferMemory() {
		return bufferMemory;
	}

	public void setBufferMemory(long bufferMemory) {
		this.bufferMemory = bufferMemory;
	}


}
