package com.alibaba.otter.shared.common.model.config.data.hdfs;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

public class HDFSMediaSource extends DataMediaSource {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7855925547410085598L;
	private String url;
	private String username;
	private String password;
	private String dir;
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
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
	public String getDir() {
		return dir;
	}
	public void setDir(String dir) {
		this.dir = dir;
	}

}
