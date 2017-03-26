package com.alibaba.otter.shared.common.model.config.data.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.ServerPort;

public class ElasticSearchMediaSource extends DataMediaSource {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1062999442413159135L;
	private String username;
	private String password;
	private List<ServerPort> servers=new ArrayList<ServerPort>();
	private String clusterName;
	private String indexName;
	
	public String getIndexName() {
		return indexName;
	}
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public List<ServerPort> getServers() {
		return servers;
	}
	public void setServers(List<ServerPort> servers) {
		this.servers = servers;
	}
	
	
}
