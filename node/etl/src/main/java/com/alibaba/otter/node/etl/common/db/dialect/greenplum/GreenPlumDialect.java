package com.alibaba.otter.node.etl.common.db.dialect.greenplum;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

import com.alibaba.otter.node.etl.common.db.dialect.AbstractDbDialect;

@SuppressWarnings("unchecked")
public class GreenPlumDialect extends AbstractDbDialect {

	public GreenPlumDialect(JdbcTemplate jdbcTemplate, LobHandler lobHandler) {
		super(jdbcTemplate, lobHandler);
		sqlTemplate = new GreenPlumSqlTemplate();
	}

	public GreenPlumDialect(JdbcTemplate jdbcTemplate, LobHandler lobHandler, String name, int majorVersion,
			int minorVersion) {
		super(jdbcTemplate, lobHandler, name, majorVersion, minorVersion);
		sqlTemplate = new GreenPlumSqlTemplate();
	}

	@Override
	public String getDefaultSchema() {
		return null;
	}

	public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return true;
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public boolean isSupportMergeSql() {
        return true;
    }

    public String getDefaultCatalog() {
        return null;
    }

}
