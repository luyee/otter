package com.alibaba.otter.shared.common.model.config.data.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.ServerPort;

public class CassandraMediaSource extends DataMediaSource {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5645258951685781273L;

	private String username;
	private String password;
	private List<ServerPort> servers=new ArrayList<ServerPort>();

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


	public void addServerPort(ServerPort server) {
		if (servers == null) {
			servers = new ArrayList<ServerPort>();
		}
		servers.add(server);
	}

	public List<ServerPort> getServers() {
		return servers;
	}

	public void setServers(List<ServerPort> servers) {
		this.servers = servers;
	}

}
