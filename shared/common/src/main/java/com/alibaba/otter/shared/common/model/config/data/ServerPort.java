package com.alibaba.otter.shared.common.model.config.data;

public class ServerPort {

	private String server;
	private Integer port=9042;
	
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	
}
