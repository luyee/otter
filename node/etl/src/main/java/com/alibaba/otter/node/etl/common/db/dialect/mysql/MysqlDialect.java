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

package com.alibaba.otter.node.etl.common.db.dialect.mysql;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.util.Assert;

import com.alibaba.otter.node.etl.common.db.dialect.AbstractDbDialect;
import com.alibaba.otter.shared.common.utils.meta.DdlUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * 基于mysql的一些特殊处理定义
 * 
 * @author jianghang 2011-10-27 下午01:46:57
 * @version 4.0.0
 */
public class MysqlDialect extends AbstractDbDialect {

	private boolean isDRDS = false;
	private LoadingCache<List<String>, String> shardColumns;

	public MysqlDialect(JdbcTemplate jdbcTemplate, LobHandler lobHandler) {
		super(jdbcTemplate, lobHandler);
		sqlTemplate = new MysqlSqlTemplate();
	}

	public MysqlDialect(JdbcTemplate jdbcTemplate, LobHandler lobHandler, String name, String databaseVersion,
			int majorVersion, int minorVersion) {
		super(jdbcTemplate, lobHandler, name, majorVersion, minorVersion);
		sqlTemplate = new MysqlSqlTemplate();

		if (StringUtils.contains(databaseVersion, "-TDDL-")) {
			isDRDS = true;
			initShardColumns();
		}
	}

	private void initShardColumns() {
		// soft引用设置，避免内存爆了
		CacheBuilder<List<String>, String> mapMaker = CacheBuilder.newBuilder().maximumSize(1000).softValues()
				.removalListener(new RemovalListener<List<String>, String>() {
					@Override
					public void onRemoval(RemovalNotification<List<String>, String> notification) {
						logger.warn("Eviction For Table:" + notification.getValue());

					}
				});

		this.shardColumns = mapMaker.build(new CacheLoader<List<String>, String>() {

			public String load(List<String> names) {
				Assert.isTrue(names.size() == 2);
				try {
					String result = DdlUtils.getShardKeyByDRDS(jdbcTemplate, names.get(0), names.get(0), names.get(1));
					if (StringUtils.isEmpty(result)) {
						return "";
					} else {
						return result;
					}
				} catch (Exception e) {
					throw new NestableRuntimeException("find table [" + names.get(0) + "." + names.get(1) + "] error",
							e);
				}
			}
		});
	}

	public boolean isCharSpacePadded() {
		return false;
	}

	public boolean isCharSpaceTrimmed() {
		return true;
	}

	public boolean isEmptyStringNulled() {
		return false;
	}

	public boolean isSupportMergeSql() {
		return true;
	}

	public String getDefaultSchema() {
		return null;
	}

	public boolean isDRDS() {
		return isDRDS;
	}

	public String getShardColumns(String schema, String table) {
		if (isDRDS()) {
			try {
				return shardColumns.get(Arrays.asList(schema, table));
			} catch (ExecutionException e) {
				e.printStackTrace();
				return null;
			}
		} else {
			return null;
		}
	}

	public String getDefaultCatalog() {
		return (String) jdbcTemplate.queryForObject("select database()", String.class);
	}

}
