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

package com.alibaba.otter.node.etl.common.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.node.etl.common.db.dialect.kafka.KafkaDialect;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author jianghang 2011-10-27 下午02:12:06
 * @version 4.0.0
 */
public class DbDialectFactory implements DisposableBean {

    private static final Logger                      logger = LoggerFactory.getLogger(DbDialectFactory.class);
    private DataSourceService                        dataSourceService;
    private DbDialectGenerator                       dbDialectGenerator;

 // 第一层pipelineId , 第二层DbMediaSource id
 	private LoadingCache<Long, LoadingCache<DataMediaSource, DbDialect>> dialects;
 	
    public DbDialectFactory(){
        // 构建第一层map
    			CacheBuilder<Long, LoadingCache<DataMediaSource, DbDialect>> cacheBuilder = CacheBuilder.newBuilder().maximumSize(1000)
    					.softValues().removalListener(new RemovalListener<Long, LoadingCache<DataMediaSource, DbDialect>>() {

    						@Override
    						public void onRemoval(
    								RemovalNotification<Long, LoadingCache<DataMediaSource, DbDialect>> paramRemovalNotification) {
    							if (paramRemovalNotification.getValue() == null) {
    								return;
    							}

    							for (DbDialect dbDialect : paramRemovalNotification.getValue().asMap().values()) {
    								dbDialect.destory();
    							}
    						}
    					});


        dialects =cacheBuilder.build(new CacheLoader<Long, LoadingCache<DataMediaSource, DbDialect>>() {

            public LoadingCache<DataMediaSource, DbDialect> load(final Long pipelineId) {
                // 构建第二层map
                return CacheBuilder.newBuilder().maximumSize(1000)
						.build(new CacheLoader<DataMediaSource, DbDialect>() {

                    @SuppressWarnings("unchecked")
					public DbDialect load(final DataMediaSource source) {
                    	if(source.getType().isKafka()){
                    		KafkaDialect kafkaDialect = new KafkaDialect(dataSourceService.getDataSource(pipelineId, source), "", 0, 0);
                    		return kafkaDialect;
                    	}
                        DataSource dataSource = dataSourceService.getDataSource(pipelineId, source);
                        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                        return (DbDialect) jdbcTemplate.execute(new ConnectionCallback() {

                            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                                DatabaseMetaData meta = c.getMetaData();
                                String databaseName = meta.getDatabaseProductName();
                                String databaseVersion = meta.getDatabaseProductVersion();
                                int databaseMajorVersion = meta.getDatabaseMajorVersion();
                                int databaseMinorVersion = meta.getDatabaseMinorVersion();
                                DbDialect dialect = dbDialectGenerator.generate(jdbcTemplate,
                                    databaseName,
                                    databaseVersion,
                                    databaseMajorVersion,
                                    databaseMinorVersion,
                                    source.getType());
                                if (dialect == null) {
                                    throw new UnsupportedOperationException("no dialect for" + databaseName);
                                }

                                if (logger.isInfoEnabled()) {
                                    logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---",
                                        databaseName,
                                        (dialect.getDefaultSchema() == null) ? dialect.getDefaultCatalog() : dialect.getDefaultSchema()));
                                }

                                return dialect;
                            }
                        });

                    }
                });
            }
        });

    }
    
   // public DbDialect getDbDialect(Long pipelineId, DataMediaSource source) {
    	public DbDialect getDbDialect(Long pipelineId, DataMediaSource source) {
        try {
			return dialects.get(pipelineId).get(source);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return null;
		}
    }

    public void destory(Long pipelineId) {
		try {
			LoadingCache<DataMediaSource, DbDialect>  dialect = dialects.get(pipelineId);
			if (dialect != null) {
	            for (DbDialect dbDialect : dialect.asMap().values()) {
	                dbDialect.destory();
	            }
	        }
	        dialects.invalidate(pipelineId);
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
        
    }

    public void destroy() throws Exception {
        Set<Long> pipelineIds = new HashSet<Long>(dialects.asMap().keySet());
        for (Long pipelineId : pipelineIds) {
            destory(pipelineId);
        }
    }

    // =============== setter / getter =================

    public void setDataSourceService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    public void setDbDialectGenerator(DbDialectGenerator dbDialectGenerator) {
        this.dbDialectGenerator = dbDialectGenerator;
    }

}
