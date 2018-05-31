package com.alibaba.otter.manager.biz.common;

import com.alibaba.otter.common.push.datasource.DataSourceHanlder;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.model.config.data.kafka.KafkaMediaSource;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.client.transport.TransportClient.Builder;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

public class DataSourceCreator
  implements DisposableBean
{
  private static final Logger logger = LoggerFactory.getLogger(DataSourceCreator.class);
  
  private int maxWait = 60000;
  
  private int minIdle = 0;
  
  private int initialSize = 1;
  
  private int maxActive = 5;
  
  private int maxIdle = 1;
  
  private int numTestsPerEvictionRun = -1;
  
  private int timeBetweenEvictionRunsMillis = 60000;
  
  private int removeAbandonedTimeout = 600;
  
  private int minEvictableIdleTimeMillis = 1800000;
  
  private List<DataSourceHanlder> dataSourceHandlers;
  
  private Map<String, Client> esclientMap = new ConcurrentHashMap();
  private Map<String, Connection> hbaseConnMap = new ConcurrentHashMap();
  private Map<String, Configuration> configurationMap = new ConcurrentHashMap();
  private Map<String, Producer> kafkaProducerMap = new ConcurrentHashMap();
  
  private Map<String, Cluster> clusterMap = new ConcurrentHashMap();
  
  public DataSource createDataSource(DataMediaSource dataMediaSource)
  {
    Assert.notNull(dataMediaSource);
    DbMediaSource dbMediaSource = (DbMediaSource)dataMediaSource;
    
    DataSource customDataSource = preCreate(Long.valueOf(0L), dbMediaSource);
    if (customDataSource != null) {
      return customDataSource;
    }
    
    return createDataSource(dbMediaSource.getUrl(), dbMediaSource.getUsername(), dbMediaSource.getPassword(), dbMediaSource
      .getDriver(), dbMediaSource.getType(), dbMediaSource.getEncode());
  }
  
  public Cluster getCluster(DataMediaSource dataMediaSource)
  {
    Assert.notNull(dataMediaSource);
    DbMediaSource dbMediaSource = (DbMediaSource)dataMediaSource;
    Cluster cluster = (Cluster)this.clusterMap.get(dbMediaSource.getName());
    if (cluster == null) {
      String[] ips = StringUtils.split(dbMediaSource.getUrl(), ";");
      Cluster.Builder builder = Cluster.builder();
      for (String ip : ips) {
        String[] ports = StringUtils.split(ip, ":");
        if (ports.length == 2) {
          builder.addContactPoint(ports[0]).withPort(NumberUtils.toInt(ports[1], 9042));
        } else {
          builder.addContactPoint(ip);
        }
      }
      if (StringUtils.isEmpty(dbMediaSource.getUsername())) {
        cluster = builder.build();
      } else {
        cluster = builder.withCredentials(dbMediaSource.getUsername(), dbMediaSource.getPassword()).build();
      }
      Metadata metadata = cluster.getMetadata();
      logger.info("Connected to cluster: %s\n", metadata.getClusterName());
      for (Host host : metadata.getAllHosts()) {
        logger.info("Datatacenter: %s; Host: %s; Rack: %s\n", new Object[] { host.getDatacenter(), host.getAddress(), host.getRack() });
      }
      
      this.clusterMap.put(dbMediaSource.getName(), cluster);
    }
    return cluster;
  }
  
  public Configuration getConf(String dir, String ugi, String conf)
    throws IOException
  {
    URI uri = null;
    Configuration cfg = null;
    String scheme = null;
    try {
      uri = new URI(dir);
      scheme = uri.getScheme();
      if (null == scheme) {
        throw new IOException("HDFS Path missing scheme, check path begin with hdfs://ip:port/ .");
      }
      cfg = (Configuration)this.configurationMap.get(scheme);
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
    if (cfg == null) {
      cfg = new Configuration();
      cfg.setClassLoader(DataSourceCreator.class.getClassLoader());
      if ((!StringUtils.isBlank(conf)) && (new File(conf).exists())) {
        cfg.addResource(new Path(conf));
      }
      if (uri.getScheme() != null) {
        String fsname = String.format("%s://%s:%s", new Object[] { uri.getScheme(), uri.getHost(), Integer.valueOf(uri.getPort()) });
        cfg.set("fs.default.name", fsname);
      }
      if (ugi != null) {
        cfg.set("hadoop.job.ugi", ugi);
      }
      this.configurationMap.put(scheme, cfg);
    }
    return cfg;
  }
  
  public FileSystem getHDFS(DataMediaSource dataMediaSource)
    throws IOException
  {
    Assert.notNull(dataMediaSource);
    DbMediaSource dbMediaSource = (DbMediaSource)dataMediaSource;
    String[] urls = StringUtils.split(dbMediaSource.getUrl(), "||");
    FileSystem fs = FileSystem.get(getConf(urls[0], dbMediaSource.getUsername(), urls[1]));
    if (fs.exists(new Path(urls[0]))) {
      return fs;
    }
    return null;
  }
  
  public Producer getProducer(DataMediaSource dataMediaSource)
  {
    Assert.notNull(dataMediaSource);
    
    KafkaMediaSource dbMediaSource = (KafkaMediaSource)dataMediaSource;
    Producer kp = (Producer)this.kafkaProducerMap.get(dbMediaSource.getName());
    if (kp == null) {
      Properties props = new Properties();
      String[] urls = StringUtils.split(dbMediaSource.getUrl(), "|");
      props.put("bootstrap.servers", urls[0]);
      props.put("metadata.broker.list", urls[0]);
      if (urls.length == 2) {
        props.put("zookeeper.connect", urls[1]);
      }
      props.put("acks", "all");
      props.put("retries", Integer.valueOf(0));
      props.put("batch.size", Integer.valueOf(16384));
      props.put("linger.ms", Integer.valueOf(1));
      props.put("buffer.memory", Integer.valueOf(33554432));
      props.put("partitioner.class", "com.alibaba.otter.node.etl.common.db.dialect.kafka.HashCodePartitioner");
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      
      props.put("key.serializer.class", "kafka.serializer.StringEncoder");
      
      kp = new KafkaProducer(props);
      this.kafkaProducerMap.put(dbMediaSource.getName(), kp);
    }
    return kp;
  }
  
  public Connection getHBaseConnection(DataMediaSource dataMediaSource) throws IOException {
    Assert.notNull(dataMediaSource);
    DbMediaSource dbMediaSource = (DbMediaSource)dataMediaSource;
    Connection connection = (Connection)this.hbaseConnMap.get(dbMediaSource.getName());
    if (connection == null) {
      Configuration conf = HBaseConfiguration.create();
      conf.addResource(new Path(dbMediaSource.getUrl()));
      if (StringUtils.isNotEmpty(dbMediaSource.getUsername())) {
        System.setProperty("HADOOP_USER_NAME", dbMediaSource.getUsername());
      }
      connection = ConnectionFactory.createConnection(conf);
      this.hbaseConnMap.put(dbMediaSource.getName(), connection);
    }
    return connection;
  }
  
  public Client getClient(DataMediaSource dataMediaSource)
  {
    Assert.notNull(dataMediaSource);
    DbMediaSource dbMediaSource = (DbMediaSource)dataMediaSource;
    Client client = (Client)this.esclientMap.get(dbMediaSource.getName());
    if (client == null) {
      String[] urls = StringUtils.split(dbMediaSource.getUrl(), "||");
      if (urls.length != 3) return null;
      String[] hosts = StringUtils.split(urls[0], ";");
      Settings settings = null;
      TransportClient tclinet = null;
      if (StringUtils.isNotEmpty(dbMediaSource.getUsername()))
      {
        settings = Settings.settingsBuilder().put("cluster.name", urls[1]).put("shield.user", dbMediaSource.getUsername() + ":" + dbMediaSource.getPassword()).put("client.transport.sniff", true).build();
        
        tclinet = TransportClient.builder().addPlugin(ShieldPlugin.class).addPlugin(DeleteByQueryPlugin.class).settings(settings).build();
      }
      else {
        settings = Settings.settingsBuilder().put("cluster.name", urls[1]).put("client.transport.sniff", true).build();
        tclinet = TransportClient.builder().addPlugin(DeleteByQueryPlugin.class).settings(settings).build();
      }
      for (String host : hosts) {
        String[] hp = StringUtils.split(host, ":");
        try {
          tclinet.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hp[0]), 
            NumberUtils.toInt(hp[1], 9300)));
        } catch (UnknownHostException e) {
          return null;
        }
      }
      IndicesExistsResponse response = (IndicesExistsResponse)tclinet.admin().indices().prepareExists(new String[] { urls[2] }).execute().actionGet();
      if (!response.isExists()) {
        return null;
      }
      this.esclientMap.put(dbMediaSource.getName(), tclinet);
    }
    return client;
  }
  
  public void destroyDataSource(DataSource dataSource)
  {
    try {
      if (letHandlerDestroyIfSupport(Long.valueOf(0L), dataSource)) {
        return;
      }
      
      if (dataSource == null) {
        return;
      }
      
      BasicDataSource basicDataSource = (BasicDataSource)dataSource;
      basicDataSource.close();
    } catch (SQLException e) {
      logger.error("ERROR ## close the datasource has an error", e);
    }
  }
  
  public void destroy()
    throws Exception
  {}
  
  private DataSource preCreate(Long pipelineId, DbMediaSource dbMediaSource)
  {
    if (CollectionUtils.isEmpty(this.dataSourceHandlers)) {
      return null;
    }
    
    DataSource dataSource = null;
    for (DataSourceHanlder handler : this.dataSourceHandlers) {
      if (handler.support(dbMediaSource)) {
        dataSource = handler.create(pipelineId, dbMediaSource);
        if (dataSource != null) {
          return dataSource;
        }
      }
    }
    return null;
  }
  
  public boolean letHandlerDestroyIfSupport(Long pipelineId, DataSource source) {
    boolean destroied = false;
    
    if (CollectionUtils.isEmpty(this.dataSourceHandlers)) {
      return destroied;
    }
    
    for (DataSourceHanlder handler : this.dataSourceHandlers) {
      if (handler.support(source)) {
        handler.destory(pipelineId);
        destroied = true;
        return destroied;
      }
    }
    return destroied;
  }
  
  private DataSource createDataSource(String url, String userName, String password, String driverClassName, DataMediaType dataMediaType, String encoding)
  {
    BasicDataSource dbcpDs = new BasicDataSource();
    
    dbcpDs.setInitialSize(this.initialSize);
    dbcpDs.setMaxActive(this.maxActive);
    dbcpDs.setMaxIdle(this.maxIdle);
    dbcpDs.setMinIdle(this.minIdle);
    dbcpDs.setMaxWait(this.maxWait);
    dbcpDs.setRemoveAbandoned(true);
    dbcpDs.setLogAbandoned(true);
    dbcpDs.setRemoveAbandonedTimeout(this.removeAbandonedTimeout);
    dbcpDs.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
    dbcpDs.setTestOnBorrow(false);
    dbcpDs.setTestOnReturn(false);
    dbcpDs.setTestWhileIdle(true);
    dbcpDs.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);
    dbcpDs.setMinEvictableIdleTimeMillis(this.minEvictableIdleTimeMillis);
    
    dbcpDs.setDriverClassName(driverClassName);
    dbcpDs.setUrl(url);
    dbcpDs.setUsername(userName);
    dbcpDs.setPassword(password);
    
    if (dataMediaType.isOracle()) {
      dbcpDs.addConnectionProperty("restrictGetTables", "true");
    }
    else if (dataMediaType.isMysql())
    {
      dbcpDs.addConnectionProperty("useServerPrepStmts", "false");
      dbcpDs.addConnectionProperty("rewriteBatchedStatements", "true");
      dbcpDs.addConnectionProperty("zeroDateTimeBehavior", "convertToNull");
      dbcpDs.addConnectionProperty("yearIsDateType", "false");
      dbcpDs.addConnectionProperty("noDatetimeStringSync", "true");
      if (StringUtils.isNotEmpty(encoding)) {
        if (StringUtils.equalsIgnoreCase(encoding, "utf8mb4")) {
          dbcpDs.addConnectionProperty("characterEncoding", "utf8");
          dbcpDs.setConnectionInitSqls(Arrays.asList(new String[] { "set names utf8mb4" }));
        } else {
          dbcpDs.addConnectionProperty("characterEncoding", encoding);
        }
      }
    }
    else {
      logger.error("ERROR ## Unknow database type");
    }
    
    return dbcpDs;
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
