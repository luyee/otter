package com.alibaba.otter.node.etl.common.db.dialect;

public final class NosqlTemplateUtil {

	
	 private static final String DOT = ".";

	    public String getSelectSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
	        StringBuilder sql = new StringBuilder("select ");
	        int size = columnNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(appendEscape(columnNames[i])).append((i + 1 < size) ? " , " : "");
	        }

	        sql.append(" from ").append(getFullName(schemaName, tableName)).append(" where ( ");
	        appendColumnEquals(sql, pkNames, "and");
	        sql.append(" ) ");
	        return sql.toString().intern();// 不使用intern，避免方法区内存消耗过多
	    }

	    public static String getUpdateSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
	        StringBuilder sql = new StringBuilder("update " + getFullName(schemaName, tableName) + " set ");
	        appendColumnEquals(sql, columnNames, ",");
	        sql.append(" where (");
	        appendColumnEquals(sql, pkNames, "and");
	        sql.append(")");
	        return sql.toString().intern(); // 不使用intern，避免方法区内存消耗过多
	    }

	    public static String getInsertSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
	        StringBuilder sql = new StringBuilder("insert into " + getFullName(schemaName, tableName) + "(");
	        String[] allColumns = new String[pkNames.length + columnNames.length];
	        System.arraycopy(columnNames, 0, allColumns, 0, columnNames.length);
	        System.arraycopy(pkNames, 0, allColumns, columnNames.length, pkNames.length);

	        int size = allColumns.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(appendEscape(allColumns[i])).append((i + 1 < size) ? "," : "");
	        }

	        sql.append(") values (");
	        appendColumnQuestions(sql, allColumns);
	        sql.append(")");
	        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
	    }

	    public static String getDeleteSql(String schemaName, String tableName, String[] pkNames) {
	        StringBuilder sql = new StringBuilder("delete from " + getFullName(schemaName, tableName) + " where ");
	        appendColumnEquals(sql, pkNames, "and");
	        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
	    }

	    protected static String getFullName(String schemaName, String tableName) {
	        StringBuilder sb = new StringBuilder();
	        if (schemaName != null) {
	            sb.append(appendEscape(schemaName)).append(DOT);
	        }
	        sb.append(appendEscape(tableName));
	        return sb.toString().intern();
	    }

	    // ================ helper method ============

	    protected static String appendEscape(String columnName) {
	        return columnName;
	    }

	    protected static void appendColumnQuestions(StringBuilder sql, String[] columns) {
	        int size = columns.length;
	        for (int i = 0; i < size; i++) {
	            sql.append("?").append((i + 1 < size) ? " , " : "");
	        }
	    }

	    protected static void appendColumnEquals(StringBuilder sql, String[] columns, String separator) {
	        int size = columns.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(" ").append(appendEscape(columns[i])).append(" = ").append("? ");
	            if (i != size - 1) {
	                sql.append(separator);
	            }
	        }
	    }
	    
	    
	    private static final String ESCAPE = "`";

	    public static String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] columnNames,
	                              String[] viewColumnNames, boolean includePks) {
	        StringBuilder sql = new StringBuilder("insert into " + getFullName(schemaName, tableName) + "(");
	        int size = columnNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(appendEscape(columnNames[i])).append(" , ");
	        }
	        size = pkNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(appendEscape(pkNames[i])).append((i + 1 < size) ? " , " : "");
	        }

	        sql.append(") values (");
	        size = columnNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append("?").append(" , ");
	        }
	        size = pkNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append("?").append((i + 1 < size) ? " , " : "");
	        }
	        sql.append(")");
	        sql.append(" on duplicate key update ");

	        size = columnNames.length;
	        for (int i = 0; i < size; i++) {
	            sql.append(appendEscape(columnNames[i]))
	                .append("=values(")
	                .append(appendEscape(columnNames[i]))
	                .append(")");
	            if (includePks) {
	                sql.append(" , ");
	            } else {
	                sql.append((i + 1 < size) ? " , " : "");
	            }
	        }

	        if (includePks) {
	            // mysql merge sql匹配了uniqe / primary key时都会执行update，所以需要更新pk信息
	            size = pkNames.length;
	            for (int i = 0; i < size; i++) {
	                sql.append(appendMergeEscape(pkNames[i])).append("=values(").append(appendMergeEscape(pkNames[i])).append(")");
	                sql.append((i + 1 < size) ? " , " : "");
	            }
	        }

	        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
	    }

	    protected static String appendMergeEscape(String columnName) {
	        return ESCAPE + columnName + ESCAPE;
	    }
}
