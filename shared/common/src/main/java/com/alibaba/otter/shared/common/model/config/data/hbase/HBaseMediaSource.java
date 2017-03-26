package com.alibaba.otter.shared.common.model.config.data.hbase;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

public class HBaseMediaSource extends DataMediaSource {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3201945572800522377L;

	private String hbaseSitePath;

	public String getHbaseSitePath() {
		return hbaseSitePath;
	}

	public void setHbaseSitePath(String hbaseSitePath) {
		this.hbaseSitePath = hbaseSitePath;
	}
	
	private String userName;
	
	private String password;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
}
