/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.common.datasource.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.common.push.datasource.DataSourceHanlder;
import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.ServerPort;
import com.alibaba.otter.shared.common.model.config.data.cassandra.CassandraMediaSource;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.model.config.data.elasticsearch.ElasticSearchMediaSource;
import com.alibaba.otter.shared.common.model.config.data.hbase.HBaseMediaSource;
import com.alibaba.otter.shared.common.model.config.data.hdfs.HDFSMediaSource;
import com.alibaba.otter.shared.common.model.config.data.kafka.KafkaMediaSource;
import com.alibaba.otter.shared.common.model.config.data.mq.MqMediaSource;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.shade.com.alibaba.fastjson.JSON;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Comment of DataSourceServiceImpl
 * 
 * @author xiaoqing.zhouxq
 * @author zebinxu, add {@link DataSourceHanlder}
 */
public class DBDataSourceService implements DataSourceService, DisposableBean {

	private static final Logger logger = LoggerFactory.getLogger(DBDataSourceService.class);

	private List<DataSourceHanlder> dataSourceHandlers;

	private int maxWait = 60 * 1000;

	private int minIdle = 0;

	private int initialSize = 0;

	private int maxActive = 32;

	private int maxIdle = 32;

	private int numTestsPerEvictionRun = -1;

	private int timeBetweenEvictionRunsMillis = 60 * 1000;

	private int removeAbandonedTimeout = 5 * 60;

	private int minEvictableIdleTimeMillis = 5 * 60 * 1000;

	/**
	 * 一个pipeline下面有一组DataSource.<br>
	 * key = pipelineId<br>
	 * value = key(dataMediaSourceId)-value(DataSource)<br>
	 */
	private LoadingCache<Long, LoadingCache<DataMediaSource, Object>> dataSources;

	public DBDataSourceService() {
		// 设置soft策略
		CacheBuilder<Long, LoadingCache<DataMediaSource, Object>> cacheBuilder = CacheBuilder.newBuilder().softValues()
				.removalListener(new RemovalListener<Long, LoadingCache<DataMediaSource, Object>>() {
					@Override
					public void onRemoval(RemovalNotification<Long, LoadingCache<DataMediaSource, Object>> paramR) {
						if (dataSources == null) {
							return;
						}
						for (Object dbconn : paramR.getValue().asMap().values()) {
							try {
								if (dbconn instanceof DataSource) {
									DataSource source = (DataSource) dbconn;
									// for filter to destroy custom datasource
									if (letHandlerDestroyIfSupport(paramR.getKey(), source)) {
										continue;
									}
									BasicDataSource basicDataSource = (BasicDataSource) source;
									basicDataSource.close();
								} else if (dbconn instanceof org.elasticsearch.client.Client) {
									org.elasticsearch.client.Client client = (org.elasticsearch.client.Client) dbconn;
									client.close();
								} else if (dbconn instanceof org.apache.hadoop.hbase.client.Connection) {
									org.apache.hadoop.hbase.client.Connection hbaseconn = (org.apache.hadoop.hbase.client.Connection) dbconn;
									hbaseconn.close();
								} else if (dbconn instanceof org.apache.kafka.clients.producer.Producer) {
									@SuppressWarnings("rawtypes")
									org.apache.kafka.clients.producer.Producer producer = (org.apache.kafka.clients.producer.Producer) dbconn;
									producer.close();
								} else if (dbconn instanceof com.datastax.driver.core.Cluster) {
									com.datastax.driver.core.Cluster producer = (com.datastax.driver.core.Cluster) dbconn;
									producer.close();
								} else if (dbconn instanceof org.apache.hadoop.fs.FileSystem) {
									org.apache.hadoop.fs.FileSystem filesystem = (org.apache.hadoop.fs.FileSystem) dbconn;
									filesystem.close();
								} else if (dbconn instanceof com.alibaba.rocketmq.client.producer.MQProducer) {
									com.alibaba.rocketmq.client.producer.MQProducer mqProducer = (com.alibaba.rocketmq.client.producer.MQProducer) dbconn;
									mqProducer.shutdown();
								}
							} catch (SQLException | IOException e) {
								logger.error("ERROR ## close the datasource has an error", e);
							}
						}
					}
				});

		// 构建第一层map
		dataSources = cacheBuilder.build(new CacheLoader<Long, LoadingCache<DataMediaSource, Object>>() {
			@Override
			public LoadingCache<DataMediaSource, Object> load(final Long pipelineId) throws Exception {
				return CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<DataMediaSource, Object>() {
					@Override
					public Object load(DataMediaSource dataMediaSource) throws Exception {
						// 扩展功能,可以自定义一些自己实现的 dataSource
						if (dataMediaSource.getType().isCassandra()) {
							CassandraMediaSource cassandraMediaSource=(CassandraMediaSource)dataMediaSource;
							return getCassandraCluster(cassandraMediaSource);
						} else if (dataMediaSource.getType().isElasticSearch()) {
							ElasticSearchMediaSource esMediaSource=(ElasticSearchMediaSource)dataMediaSource;
							return getElasticSearchClient(esMediaSource);
						} else if (dataMediaSource.getType().isHbase()) {
							HBaseMediaSource hbMediaSource=(HBaseMediaSource)dataMediaSource;
							return getHBaseConnection(hbMediaSource);
						} else if (dataMediaSource.getType().isHDFSArvo()) {
							HDFSMediaSource hdfsMediaSource=(HDFSMediaSource)dataMediaSource;
							return getHDFS(hdfsMediaSource);
						} else if (dataMediaSource.getType().isKafka()) {
							KafkaMediaSource kafkaMediaSource=(KafkaMediaSource)dataMediaSource;
							return getKafkaProducer(kafkaMediaSource);
						} else if (dataMediaSource.getType().isMq()) {
							MqMediaSource mqMediaSource=(MqMediaSource)dataMediaSource;
							return getMetaQProducer(mqMediaSource);
						} else {
							DbMediaSource dbMediaSource=(DbMediaSource)dataMediaSource;
							DataSource customDataSource = preCreate(pipelineId, dbMediaSource);
							if (customDataSource != null) {
								return customDataSource;
							}
							return createDataSource(dbMediaSource.getUrl(), dbMediaSource.getUsername(),
									dbMediaSource.getPassword(), dbMediaSource.getDriver(), dbMediaSource.getType(),
									dbMediaSource.getEncode());
						}
					}
					
				});
			}
		});
	}
	/**
	 * 建立MetaQ消息生产者
	 * @param mqMediaSource
	 * @return
	 */
	public com.alibaba.rocketmq.client.producer.MQProducer getMetaQProducer(MqMediaSource mqMediaSource) {
		com.alibaba.rocketmq.client.producer.DefaultMQProducer producer = new com.alibaba.rocketmq.client.producer.DefaultMQProducer(  
				mqMediaSource.getProducerGroupName());  
        //nameserver服务,多个以;分开  
        producer.setNamesrvAddr(mqMediaSource.getNamesrvAddr());  
        producer.setInstanceName(mqMediaSource.getInstanceName());  
        try {
			producer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		} 
        return producer;
	}
	
	/**
	 * 获取cassdran的连接
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	public com.datastax.driver.core.Cluster getCassandraCluster(CassandraMediaSource cassandraMediaSource) {
		Assert.notNull(cassandraMediaSource);
		com.datastax.driver.core.Cluster cluster = null;
		com.datastax.driver.core.Cluster.Builder builder=com.datastax.driver.core.Cluster.builder();
		for(ServerPort server:cassandraMediaSource.getServers()){
			if (server.getPort()>0){
				builder.addContactPoint(server.getServer()).withPort(server.getPort());
			}else{
				builder.addContactPoint(server.getServer());
			}
		}
		if (StringUtils.isEmpty(cassandraMediaSource.getUsername())) {
			cluster = builder.build();
		} else {
			cluster = builder.withCredentials(cassandraMediaSource.getUsername(), cassandraMediaSource.getPassword()).build();
		}
		com.datastax.driver.core.Metadata metadata = cluster.getMetadata();
		logger.info("Connected to cluster: %s\n", metadata.getClusterName());
		for (com.datastax.driver.core.Host host : metadata.getAllHosts()) {
			logger.info("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		return cluster;
	}

	/**
	 * hdfs建立config
	 * 
	 * @param dir
	 * @param ugi
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public org.apache.hadoop.conf.Configuration getConf(String dir, String ugi, String conf) throws IOException {
		URI uri = null;
		org.apache.hadoop.conf.Configuration cfg = null;
		String scheme = null;
		try {
			uri = new URI(dir);
			scheme = uri.getScheme();
			if (null == scheme) {
				throw new IOException("HDFS Path missing scheme, check path begin with hdfs://ip:port/ .");
			}
			cfg = new org.apache.hadoop.conf.Configuration();
			cfg.setClassLoader(DBDataSourceService.class.getClassLoader());
			if (!StringUtils.isBlank(conf) && new File(conf).exists()) {
				cfg.addResource(new org.apache.hadoop.fs.Path(conf));
			}
			if (uri.getScheme() != null) {
				String fsname = String.format("%s://%s:%s", uri.getScheme(), uri.getHost(), uri.getPort());
				cfg.set("fs.default.name", fsname);
			}
			if (ugi != null) {
				cfg.set("hadoop.job.ugi", ugi);
			}
		} catch (URISyntaxException e) {
			throw new IOException(e.getMessage(), e.getCause());
		}
		return cfg;
	}

	/**
	 * 获取hdfs配置和文件目录
	 * 
	 * @param dbMediaSource
	 * @return
	 * @throws IOException
	 */
	public org.apache.hadoop.fs.FileSystem getHDFS(HDFSMediaSource hdfsMediaSource) throws IOException {
		Assert.notNull(hdfsMediaSource);
		String[] urls = StringUtils.split(hdfsMediaSource.getUrl(), "||");
		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(getConf(urls[0], hdfsMediaSource.getUsername(), urls[1]));
		if (fs.exists(new org.apache.hadoop.fs.Path(urls[0]))) {
			return fs;
		}
		return null;
	}

	/**
	 * 获取kafka的生产者对象
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public org.apache.kafka.clients.producer.Producer getKafkaProducer(KafkaMediaSource kafkaMediaSource) {
		Assert.notNull(kafkaMediaSource);
		Properties props = new Properties();
		if (StringUtils.isNotEmpty(kafkaMediaSource.getBootstrapServers())){
			props.put("bootstrap.servers",kafkaMediaSource.getBootstrapServers());
			//props.put("metadata.broker.list", kafkaMediaSource.getBootstrapServers());
		}
		if (StringUtils.isNotEmpty(kafkaMediaSource.getZookeeperConnect())){
		//	props.put("zookeeper.connect",kafkaMediaSource.getZookeeperConnect());
		}
//		props.put("batch.size", String.valueOf(kafkaMediaSource.getBatchSize()));
//		props.put("buffer.memory", String.valueOf(kafkaMediaSource.getBufferMemory()));
		
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("request.required.acks", "1");
		
		props.put("compression.type", "gzip"); // 压缩
//		props.put("producer.type", "async");
		logger.warn(String.format("kafka props : %s", JSON.toJSONString(props)));
		org.apache.kafka.clients.producer.Producer kp = new KafkaProducer(props);
		
		return kp;
	}
	
	public static void main(String[] args){
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.28.3.158:9092,172.28.3.159:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("request.required.acks", "1");
		
		props.put("compression.type", "gzip"); // 压缩
		org.apache.kafka.clients.producer.Producer kp = new KafkaProducer(props);
		try {
			kp.send(new ProducerRecord<String, String>("test", "aaaaa"));
		} catch (Exception e) {
			System.out.println(String.format("failed %s", e));
		}
		
		
	}

	public org.apache.hadoop.hbase.client.Connection getHBaseConnection(HBaseMediaSource dbMediaSource) throws IOException {
		Assert.notNull(dbMediaSource);
		org.apache.hadoop.conf.Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
		conf.addResource(new org.apache.hadoop.fs.Path(dbMediaSource.getHbaseSitePath()));// ddResource(conf.getClass().getResourceAsStream("/hbase-site.xml"));
		System.setProperty("HADOOP_USER_NAME", dbMediaSource.getUserName());
		return  org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
	}

	/**
	 * 创建elasticSearch client
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	public org.elasticsearch.client.Client getElasticSearchClient(ElasticSearchMediaSource esMediaSource) {
		Assert.notNull(esMediaSource);
		org.elasticsearch.client.Client client = null;
		org.elasticsearch.common.transport.InetSocketTransportAddress[] transportAddress = new org.elasticsearch.common.transport.InetSocketTransportAddress[esMediaSource.getServers().size()];
		int id = 0;
		for(ServerPort server:esMediaSource.getServers()){
			try {
				transportAddress[id]= new org.elasticsearch.common.transport.InetSocketTransportAddress(InetAddress.getByName(server.getServer()),
						server.getPort());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			id++;
		}
		org.elasticsearch.common.settings.Settings settings = null;
		if (StringUtils.isNotEmpty(esMediaSource.getUsername())) {
			settings = org.elasticsearch.common.settings.Settings.settingsBuilder().put("cluster.name",esMediaSource.getClusterName())
					.put("shield.user", esMediaSource.getUsername() + ":" + esMediaSource.getPassword())
					.put("client.transport.sniff", true).build();
			client = org.elasticsearch.client.transport.TransportClient.builder().addPlugin(org.elasticsearch.shield.ShieldPlugin.class).addPlugin(org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin.class)
					.settings(settings).build().addTransportAddresses(transportAddress);
		} else {
			settings = org.elasticsearch.common.settings.Settings.settingsBuilder().put("cluster.name", esMediaSource.getClusterName())
					.put("client.transport.sniff", true).build();
			client = org.elasticsearch.client.transport.TransportClient.builder().addPlugin(org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin.class).settings(settings).build()
					.addTransportAddresses(transportAddress);
		}
		org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse response = client.admin().indices().prepareExists(esMediaSource.getIndexName()).execute().actionGet();
		if (!response.isExists()) {
			return null;
		}
		return client;
	}

	@SuppressWarnings("unchecked")
	public Object getDataSource(long pipelineId, DataMediaSource dataMediaSource) {
		Assert.notNull(dataMediaSource);
		//DbMediaSource dbMediaSource = (DbMediaSource) dataMediaSource;
		try {
			return dataSources.get(pipelineId).get(dataMediaSource);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void destroy(Long pipelineId) {
		try {
			LoadingCache<DataMediaSource, Object> sources = dataSources.get(pipelineId);
			if (sources != null) {
				for (Object dbconn : sources.asMap().values()) {
					try {
						if (dbconn instanceof DataSource) {
							DataSource source = (DataSource) dbconn;
							// for filter to destroy custom datasource
							if (letHandlerDestroyIfSupport(pipelineId, source)) {
								continue;
							}
							// fallback for regular destroy
							// TODO need to integrate to handler
							BasicDataSource basicDataSource = (BasicDataSource) source;
							basicDataSource.close();
						}
					} catch (SQLException e) {
						logger.error("ERROR ## close the datasource has an error", e);
					}
				}

				sources.invalidateAll();
				sources.cleanUp();
			}
		} catch (ExecutionException e1) {
			e1.printStackTrace();

		}
	}

	private boolean letHandlerDestroyIfSupport(Long pipelineId, DataSource dataSource) {
		boolean destroied = false;

		if (CollectionUtils.isEmpty(this.dataSourceHandlers)) {
			return destroied;
		}
		for (DataSourceHanlder handler : this.dataSourceHandlers) {
			if (handler.support(dataSource)) {
				handler.destory(pipelineId);
				destroied = true;
				return destroied;
			}
		}
		return destroied;

	}

	public void destroy() throws Exception {
		for (Long pipelineId : dataSources.asMap().keySet()) {
			destroy(pipelineId);
		}
	}

	private DataSource createDataSource(String url, String userName, String password, String driverClassName,
			DataMediaType dataMediaType, String encoding) {
		BasicDataSource dbcpDs = new BasicDataSource();

		dbcpDs.setInitialSize(initialSize);// 初始化连接池时创建的连接数
		dbcpDs.setMaxActive(maxActive);// 连接池允许的最大并发连接数，值为非正数时表示不限制
		dbcpDs.setMaxIdle(maxIdle);// 连接池中的最大空闲连接数，超过时，多余的空闲连接将会被释放，值为负数时表示不限制
		dbcpDs.setMinIdle(minIdle);// 连接池中的最小空闲连接数，低于此数值时将会创建所欠缺的连接，值为0时表示不创建
		dbcpDs.setMaxWait(maxWait);// 以毫秒表示的当连接池中没有可用连接时等待可用连接返回的时间，超时则抛出异常，值为-1时表示无限等待
		dbcpDs.setRemoveAbandoned(true);// 是否清除已经超过removeAbandonedTimeout设置的无效连接
		dbcpDs.setLogAbandoned(true);// 当清除无效链接时是否在日志中记录清除信息的标志
		dbcpDs.setRemoveAbandonedTimeout(removeAbandonedTimeout); // 以秒表示清除无效链接的时限
		dbcpDs.setNumTestsPerEvictionRun(numTestsPerEvictionRun);// 确保连接池中没有已破损的连接
		dbcpDs.setTestOnBorrow(false);// 指定连接被调用时是否经过校验
		dbcpDs.setTestOnReturn(false);// 指定连接返回到池中时是否经过校验
		dbcpDs.setTestWhileIdle(true);// 指定连接进入空闲状态时是否经过空闲对象驱逐进程的校验
		dbcpDs.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis); // 以毫秒表示空闲对象驱逐进程由运行状态进入休眠状态的时长，值为非正数时表示不运行任何空闲对象驱逐进程
		dbcpDs.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis); // 以毫秒表示连接被空闲对象驱逐进程驱逐前在池中保持空闲状态的最小时间

		// 动态的参数
		dbcpDs.setDriverClassName(driverClassName);
		dbcpDs.setUrl(url);
		dbcpDs.setUsername(userName);
		dbcpDs.setPassword(password);

		if (dataMediaType.isOracle()) {
			dbcpDs.addConnectionProperty("restrictGetTables", "true");
			dbcpDs.setValidationQuery("select 1 from dual");
		} else if (dataMediaType.isMysql()) {
			// open the batch mode for mysql since 5.1.8
			dbcpDs.addConnectionProperty("useServerPrepStmts", "false");
			dbcpDs.addConnectionProperty("rewriteBatchedStatements", "true");
			dbcpDs.addConnectionProperty("zeroDateTimeBehavior", "convertToNull");// 将0000-00-00的时间类型返回null
			dbcpDs.addConnectionProperty("yearIsDateType", "false");// 直接返回字符串，不做year转换date处理
			dbcpDs.addConnectionProperty("noDatetimeStringSync", "true");// 返回时间类型的字符串,不做时区处理
			if (StringUtils.isNotEmpty(encoding)) {
				if (StringUtils.equalsIgnoreCase(encoding, "utf8mb4")) {
					dbcpDs.addConnectionProperty("characterEncoding", "utf8");
					dbcpDs.setConnectionInitSqls(Arrays.asList("set names utf8mb4"));
				} else {
					dbcpDs.addConnectionProperty("characterEncoding", encoding);
				}
			}
			dbcpDs.setValidationQuery("select 1");
		} else {
			logger.error("ERROR ## Unknow database type");
		}

		return dbcpDs;
	}

	/**
	 * 扩展功能,可以自定义一些自己实现的 dataSource
	 */
	private DataSource preCreate(Long pipelineId, DbMediaSource dbMediaSource) {

		if (CollectionUtils.isEmpty(dataSourceHandlers)) {
			return null;
		}

		DataSource dataSource = null;
		for (DataSourceHanlder handler : dataSourceHandlers) {
			if (handler.support(dbMediaSource)) {
				dataSource = handler.create(pipelineId, dbMediaSource);
				if (dataSource != null) {
					return dataSource;
				}
			}
		}
		return null;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public void setInitialSize(int initialSize) {
		this.initialSize = initialSize;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		this.removeAbandonedTimeout = removeAbandonedTimeout;
	}

	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public void setDataSourceHandlers(List<DataSourceHanlder> dataSourceHandlers) {
		this.dataSourceHandlers = dataSourceHandlers;
	}
}
