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

package com.alibaba.otter.manager.biz.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.manager.biz.common.DataSourceCreator;
import com.alibaba.otter.manager.biz.config.datamediasource.DataMediaSourceService;
import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.ModeValueFilter;
import com.alibaba.otter.shared.common.model.config.data.DataMedia.ModeValue;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.utils.meta.DdlSchemaFilter;
import com.alibaba.otter.shared.common.utils.meta.DdlTableNameFilter;
import com.alibaba.otter.shared.common.utils.meta.DdlUtils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;


/**
 * @author simon 2011-11-25 下午04:57:55
 */
public class DataSourceChecker {

    private static final Logger    logger             = LoggerFactory.getLogger(DataSourceChecker.class);
    private DataMediaSourceService dataMediaSourceService;
    private DataSourceCreator      dataSourceCreator;

    // private static final String MYSQL_FLAG = "mysql";

    // private static final String ORACLE_FLAG = "oracle";

    // 选择的数据库类型和jdbc-url不匹配
    // private static final String DBTYPE_CONFLICT =
    // "\u9009\u62e9\u7684\u6570\u636e\u5e93\u7c7b\u578b\u548cjdbc-url\u4e0d\u5339\u914d";
    // 恭喜,数据库通过验证!
    private static final String    DATABASE_SUCCESS   = "\u606d\u559c,\u6570\u636e\u5e93\u901a\u8fc7\u9a8c\u8bc1!";
    // 抱歉,数据库未通过验证,请检查相关配置!
    private static final String    DATABASE_FAIL      = "\u62b1\u6b49,\u6570\u636e\u5e93\u672a\u901a\u8fc7\u9a8c\u8bc1,\u8bf7\u68c0\u67e5\u76f8\u5173\u914d\u7f6e!";
    // 恭喜,select操作成功,权限正常!
    private static final String    TABLE_SUCCESS      = "\u606d\u559c,select\u64cd\u4f5c\u6210\u529f,\u6743\u9650\u6b63\u5e38!";
    // 抱歉select操作报错,请检查权限配置!
    private static final String    TABLE_FAIL         = "\u62b1\u6b49,\u64cd\u4f5c\u62a5\u9519,\u8bf7\u68c0\u67e5\u6743\u9650\u914d\u7f6e!";
    // 恭喜,编码验证正确!
    private static final String    ENCODE_QUERY_ERROR = "\u6267\u884cSQL\u51fa\u9519,\u8bf7\u68c0\u67e5\u6570\u636e\u5e93\u7c7b\u578b\u662f\u5426\u9009\u62e9\u6b63\u786e!";
    // 抱歉,字符集不匹配,实际数据库默认字符集为:
    private static final String    ENCODE_FAIL        = "\u62b1\u6b49,\u5b57\u7b26\u96c6\u4e0d\u5339\u914d,\u5b9e\u9645\u6570\u636e\u5e93\u9ed8\u8ba4\u5b57\u7b26\u96c6\u4e3a:";
    // SELECT未成功
    private static final String    SELECT_FAIL        = "SELECT\u672a\u6210\u529f";

    // DELETE未成功
    // private static final String DELETE_FAIL = "DELETE\u672a\u6210\u529f";
    // INSERT未成功
    // private static final String INSERT_FAIL = "INSERT\u672a\u6210\u529f";

    // 分库前缀必须大于后缀，且不能为负数
    // private static final String SPLIT_INDEX_FAIL =
    // "\u5206\u5e93\u524d\u7f00\u5fc5\u987b\u5927\u4e8e\u540e\u7f00\uff0c\u4e14\u4e0d\u80fd\u4e3a\u8d1f\u6570";

    private void closeConnection(Connection conn) {
        closeConnection(conn, null, null);
    }

    private void closeConnection(Connection conn, Statement st) {
        closeConnection(conn, st, null);
    }

    private void closeConnection(Connection conn, Statement st, ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
            if (null != st) {
                st.close();
            }
            if (null != conn) {
                conn.close();
            }

        } catch (SQLException e) {
            logger.error("", e);
        }
    }

    @SuppressWarnings("resource")
    public String check(String name,String url, String username, String password, String encode, String sourceType) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
       DataSource dataSource = null;
        try {

            DbMediaSource dbMediaSource = new DbMediaSource();
            dbMediaSource.setName(name);
            dbMediaSource.setUrl(url);
            dbMediaSource.setUsername(username);
            dbMediaSource.setPassword(password);
            dbMediaSource.setEncode(encode);

            if (sourceType.equalsIgnoreCase("MYSQL")) {
                dbMediaSource.setType(DataMediaType.MYSQL);
                dbMediaSource.setDriver("com.mysql.jdbc.Driver");
            } else if (sourceType.equalsIgnoreCase("ORACLE")) {
                dbMediaSource.setType(DataMediaType.ORACLE);
                dbMediaSource.setDriver("oracle.jdbc.driver.OracleDriver");
            } else if (sourceType.equalsIgnoreCase("GREENPLUM")) {
                dbMediaSource.setType(DataMediaType.GREENPLUM);
                dbMediaSource.setDriver("com.pivotal.jdbc.GreenplumDriver");
            }else if (sourceType.equalsIgnoreCase("KAFKA")) {
                dbMediaSource.setType(DataMediaType.KAFKA);
            }else if (sourceType.equalsIgnoreCase("CASSANDRA")) {
                dbMediaSource.setType(DataMediaType.CASSANDRA);
            }else if (sourceType.equalsIgnoreCase("HBASE")) {
                dbMediaSource.setType(DataMediaType.HBASE);
            }else if (sourceType.equalsIgnoreCase("HDFS_ARVO")) {
                dbMediaSource.setType(DataMediaType.HDFS);
            }else if (sourceType.equalsIgnoreCase("ELASTICSEARCH")) {
                dbMediaSource.setType(DataMediaType.ELASTICSEARCH);
            }
            if (sourceType.equalsIgnoreCase("MYSQL") || sourceType.equalsIgnoreCase("greenplum") || sourceType.equalsIgnoreCase("ORACLE")){//mysql ,oracle测试
            	dataSource = dataSourceCreator.createDataSource(dbMediaSource);
                try {
                    conn = dataSource.getConnection();
                } catch (Exception e) {e.printStackTrace();
                    logger.error("check error!", e);
                }
                if (null == conn) {
                    return DATABASE_FAIL;
                }
                String sql = null;
                if (sourceType.equalsIgnoreCase("MYSQL")) {
                    sql = "SHOW VARIABLES LIKE 'character_set_database'";
                } else if (sourceType.equalsIgnoreCase("ORACLE")) {
                    sql = "select * from V$NLS_PARAMETERS where parameter in('NLS_CHARACTERSET')";
                }else if (sourceType.equalsIgnoreCase("GREENPLUM")){
                	sql="SHOW server_encoding";
                }
                stmt = conn.createStatement();
                //测试编码是否一致
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String defaultEncode = null;
                    if (sourceType.equalsIgnoreCase("MYSQL")) {
                        defaultEncode = ((String) rs.getObject(2)).toLowerCase();
                        defaultEncode = defaultEncode.equals("iso-8859-1") ? "latin1" : defaultEncode;
                        if (!encode.toLowerCase().equals(defaultEncode)) {
                            return ENCODE_FAIL + defaultEncode;
                        }
                    } else if (sourceType.equalsIgnoreCase("ORACLE")) {
                        // ORACLE查询服务器默认字符集需要管理员权限
                        defaultEncode = ((String) rs.getObject(2)).toLowerCase();
                        defaultEncode = defaultEncode.equalsIgnoreCase("zhs16gbk") ? "gbk" : defaultEncode;
                        defaultEncode = defaultEncode.equalsIgnoreCase("us7ascii") ? "iso-8859-1" : defaultEncode;
                        if (!encode.toLowerCase().equals(defaultEncode)) {
                            return ENCODE_FAIL + defaultEncode;
                        }
                    }else if (sourceType.equalsIgnoreCase("GREENPLUM")){
                    	defaultEncode = ((String) rs.getObject(1)).toLowerCase();
                    	defaultEncode = defaultEncode.equalsIgnoreCase("zhs16gbk") ? "gbk" : defaultEncode;
                        defaultEncode = defaultEncode.equalsIgnoreCase("us7ascii") ? "iso-8859-1" : defaultEncode;
                        if (!encode.toLowerCase().equals(defaultEncode)) {
                            return ENCODE_FAIL + defaultEncode;
                        }
                    }

                }
            }else if (sourceType.equalsIgnoreCase("KAFKA")){
            	if (dataSourceCreator.getProducer(dbMediaSource)==null){
            		return DATABASE_FAIL;
            	}
            }else if (sourceType.equalsIgnoreCase("CASSANDRA")){
            	if (dataSourceCreator.getCluster(dbMediaSource)==null){
            		return DATABASE_FAIL;
            	}
            }else if (sourceType.equalsIgnoreCase("HBASE")){
            	if (dataSourceCreator.getHBaseConnection(dbMediaSource)==null){
            		return DATABASE_FAIL;
            	}
            }else if (sourceType.equalsIgnoreCase("HDFS_ARVO")){
            	if (dataSourceCreator.getHDFS(dbMediaSource)==null){
            		return DATABASE_FAIL;
            	}
            }else if (sourceType.equalsIgnoreCase("ELASTICSEARCH")){
            	if (dataSourceCreator.getClient(dbMediaSource)==null){
            		return DATABASE_FAIL;
            	}
            }
        } catch (SQLException se) {
            logger.error("check error!", se);
            return ENCODE_QUERY_ERROR;
        } catch (Exception e) {e.printStackTrace();
            logger.error("check error!", e);
            return DATABASE_FAIL;
        } finally {
            closeConnection(conn);
            dataSourceCreator.destroyDataSource(dataSource);
        }

        return DATABASE_SUCCESS;

    }

    public String checkMap(String namespace, String name, Long dataSourceId) {
        Connection conn = null;
        Statement stmt = null;
        DataMediaSource source = dataMediaSourceService.findById(dataSourceId);
        DataSource dataSource = null;
        try {
            DbMediaSource dbMediaSource = (DbMediaSource) source;
            if (dbMediaSource.getType().isCassandra()){
            	Cluster cluster=dataSourceCreator.getCluster(dbMediaSource);
            	Session session=cluster.connect();
            	
            	BoundStatement bindStatement =null;
            	try{
            		bindStatement=session.prepare( "select keyspace_name,table_name from system_schema.tables where keyspace_name=? and table_name=?")
            			.bind(namespace,name);
            	}catch(InvalidQueryException iqe){
            		bindStatement=session.prepare( "select keyspace_name,columnfamily_name from system.schema_columnfamilies where keyspace_name=? and columnfamily_name=? ")
                			.bind(namespace,name);
            	}
            	com.datastax.driver.core.ResultSet resultSet = session.execute(bindStatement);
            	if (!resultSet.iterator().hasNext()) {
            		session.close();
            		return SELECT_FAIL;
            	}
            	session.close();
            }else if (dbMediaSource.getType().isElasticSearch()){
            	Client client=dataSourceCreator.getClient(source);
            	String[] urls=StringUtils.split(dbMediaSource.getUrl(), "||");
            	TypesExistsResponse typeResp = client.admin().indices().prepareTypesExists(urls[2]).setTypes(name).execute().actionGet();
            	if (!typeResp.isExists()){
            		 return SELECT_FAIL;
            	}
            }else if (dbMediaSource.getType().isHbase()){
            	Admin admin = dataSourceCreator.getHBaseConnection(dbMediaSource).getAdmin();
            	if (!admin.tableExists(TableName.valueOf(name))) {
            		return SELECT_FAIL;
            	}
            }else if (dbMediaSource.getType().isHDFSArvo()){
            	FileSystem fs=dataSourceCreator.getHDFS(dbMediaSource);
            	if (!fs.exists(new Path(name))){
            		return SELECT_FAIL;
            	}
            }else if (dbMediaSource.getType().isKafka()){
            	Producer<String,String> producer=dataSourceCreator.getProducer(dbMediaSource);
            	producer.send(new ProducerRecord<>("topic_test", dbMediaSource.getName(),dbMediaSource.getUrl()));
            }else{
	            dataSource = dataSourceCreator.createDataSource(dbMediaSource);
	            ModeValue namespaceValue = ConfigHelper.parseMode(namespace);
	            ModeValue nameValue = ConfigHelper.parseMode(name);
	            String tempNamespace = namespaceValue.getSingleValue();
	            String tempName = nameValue.getSingleValue();
	            try {
	            	Table table =null;
	            	if (dbMediaSource.getType().isGreenplum()){
	            		String[] sts=StringUtils.split(tempName, ".");
	            		table=DdlUtils.findTable(new JdbcTemplate(dataSource), tempNamespace, sts[0], sts[1]);
	            	}else{
	            		table=DdlUtils.findTable(new JdbcTemplate(dataSource), tempNamespace, tempNamespace, tempName);
	            	}
	                if (table == null) {
	                    return SELECT_FAIL;
	                }
	            } catch (SQLException se) {
	                logger.error("check error!", se);
	                return SELECT_FAIL;
	            } catch (Exception e) {
	                logger.error("check error!", e);
	                return SELECT_FAIL;
	            }
            }
        } catch (Exception e) {
			e.printStackTrace();
			return SELECT_FAIL;
		} finally {
            closeConnection(conn, stmt);
            dataSourceCreator.destroyDataSource(dataSource);
        }

        return TABLE_SUCCESS;

    }

    public String checkNamespaceTables(final String namespace, final String name, final Long dataSourceId) {
        DataSource dataSource = null;
        try {
            DataMediaSource source = dataMediaSourceService.findById(dataSourceId);
            DbMediaSource dbMediaSource = (DbMediaSource) source;
            final List<String> matchSchemaTables = new ArrayList<String>();
            matchSchemaTables.add("Find schema and tables:");
            if (dbMediaSource.getType().isCassandra()){
            	Cluster cluster=dataSourceCreator.getCluster(dbMediaSource);
            	Session session=cluster.connect();
            	BoundStatement bindStatement =null;
            	boolean isv3=true;
            	try{
            		bindStatement=session.prepare( "select keyspace_name,table_name from system_schema.tables where keyspace_name=? and table_name=?")
            			.bind(namespace,name);
            	}catch(InvalidQueryException iqe){
                    bindStatement=session.prepare( "select keyspace_name,columnfamily_name from system.schema_columnfamilies where keyspace_name=? and columnfamily_name=? ")
                        			.bind(namespace,name);
                    isv3=false;
                }
            	com.datastax.driver.core.ResultSet resultSet = session.execute(bindStatement);
            	if (!resultSet.iterator().hasNext()) {
            		session.close();
            		return TABLE_FAIL;
            	}
            	for(Row row:resultSet){
            		if (isv3){
            			matchSchemaTables.add(row.getString("keyspace_name") + "." + row.getString("table_name"));
            		}else{
            			matchSchemaTables.add(row.getString("keyspace_name") + "." + row.getString("columnfamily_name"));
            		}
            	}
            }else if (dbMediaSource.getType().isElasticSearch()){
            	Client client=dataSourceCreator.getClient(source);
            	TypesExistsResponse typeResp = client.admin().indices().prepareTypesExists(namespace).setTypes(name).execute().actionGet();
            	if (!typeResp.isExists()){
            		 return TABLE_FAIL;
            	}else{
            		 matchSchemaTables.add(namespace + "." + name);
            	}
            }else if (dbMediaSource.getType().isHbase()){
            	Admin admin = dataSourceCreator.getHBaseConnection(dbMediaSource).getAdmin();
            	if (!admin.tableExists(TableName.valueOf(name))) {
            		return TABLE_FAIL;
            	}else{
            		matchSchemaTables.add( name);
            	}
            }else if (dbMediaSource.getType().isHDFSArvo()){
            	FileSystem fs=dataSourceCreator.getHDFS(dbMediaSource);
            	if (!fs.exists(new Path(name))){
            		return TABLE_FAIL;
            	}else{
            		matchSchemaTables.add( name);
            	}
            }else if (dbMediaSource.getType().isKafka()){
            	Producer<String,String> producer=dataSourceCreator.getProducer(dbMediaSource);
            	producer.send(new ProducerRecord<>("topic_test", dbMediaSource.getName(),dbMediaSource.getUrl()));
            	matchSchemaTables.add(name);
            }else{
	            dataSource = dataSourceCreator.createDataSource(dbMediaSource);
	            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
	            List<String> schemaList;
	            {
	                ModeValue mode = ConfigHelper.parseMode(namespace);
	                String schemaPattern = ConfigHelper.makeSQLPattern(mode, namespace);
	                final ModeValueFilter modeValueFilter = ConfigHelper.makeModeValueFilter(mode, namespace);
	                if (source.getType().isOracle()||source.getType().isGreenplum()) {
	                    schemaList = Arrays.asList(namespace);
	                } else {
	                    schemaList = DdlUtils.findSchemas(jdbcTemplate, schemaPattern, new DdlSchemaFilter() {
	                        @Override
	                        public boolean accept(String schemaName) {
	                            return modeValueFilter.accept(schemaName);
	                        }
	                    });
	                }
	            }
	            if (schemaList != null) {
	            	ModeValue mode = ConfigHelper.parseMode(name);
	                String tableNamePattern = ConfigHelper.makeSQLPattern(mode, name);
	                ModeValueFilter modeValueFilter = ConfigHelper.makeModeValueFilter(mode, name);
	                for (String schema : schemaList) {
	                	String sname=schema;
	                	String tname=tableNamePattern;
	                	if (dbMediaSource.getType().isGreenplum()){
		            		String[] sts=StringUtils.split(name, ".");
		            		sname=sts[0];
		            		mode = ConfigHelper.parseMode(sts[1]);
		            		tname=ConfigHelper.makeSQLPattern(mode, sts[1]);
		            		modeValueFilter = ConfigHelper.makeModeValueFilter(mode, tname);
		            	}
	                	final ModeValueFilter mvfilter=modeValueFilter;
	                    DdlUtils.findTables(jdbcTemplate, schema, sname, tname, null, new DdlTableNameFilter() {
	                        @Override
	                        public boolean accept(String catalogName, String schemaName, String tableName) {
	                            if (mvfilter.accept(tableName)) {
	                                matchSchemaTables.add(schemaName + "." + tableName);
	                            }
	                            return false;
	                        }
	                    });
	                }
	            }
            }
            if (matchSchemaTables.size() == 1) {
                return TABLE_FAIL;
            }
            return StringUtils.join(matchSchemaTables, "<br>\n");
        } catch (Exception e) {
            logger.error("check error!", e);
            return TABLE_FAIL;
        } finally {
            dataSourceCreator.destroyDataSource(dataSource);
        }
    }

    public void setDataMediaSourceService(DataMediaSourceService dataMediaSourceService) {
        this.dataMediaSourceService = dataMediaSourceService;
    }

    public void setDataSourceCreator(DataSourceCreator dataSourceCreator) {
        this.dataSourceCreator = dataSourceCreator;
    }

}
