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

package com.alibaba.otter.shared.common.model.config.data;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.shared.common.utils.OtterToStringStyle;

/**
 * 字段同步对，描述同步映射时左右两边的字段
 * 
 * @author simon 2012-3-31 下午03:50:38
 */
public class ColumnPair implements Serializable {

    private static final long serialVersionUID = -7751579969781886333L;
    private Long              id;
    private Column            sourceColumn;
    private Column            targetColumn;
    private Long              dataMediaPairId;
    private Date              gmtCreate;
    private Date              gmtModified;
    private Integer isPk=0;//是否主键(key)，1： 是 0：否 针对hbase,cassandra,es等kv结构的key
    private String functionName;//字段值转换函数
    private Integer sourceType;//数据来源类型 0-原始字段数据 1-表名 2-当前时间 3-当前用户

    public ColumnPair(){

    }

    public ColumnPair(Column sourceColumn, Column targetColumn){
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    public ColumnPair(String sourceColumn, String targetColumn){
        this.sourceColumn = new Column(sourceColumn);
        this.targetColumn = new Column(targetColumn);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getIsPk() {
		return isPk;
	}

	public void setIsPk(Integer isPk) {
		this.isPk = isPk;
	}

	public String getFunctionName() {
		return functionName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	public Integer getSourceType() {
		return sourceType;
	}

	public void setSourceType(Integer sourceType) {
		this.sourceType = sourceType;
	}

	public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public Column getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(Column sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public Column getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(Column targetColumn) {
        this.targetColumn = targetColumn;
    }

    public Long getDataMediaPairId() {
        return dataMediaPairId;
    }

    public void setDataMediaPairId(Long dataMediaPairId) {
        this.dataMediaPairId = dataMediaPairId;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, OtterToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataMediaPairId == null) ? 0 : dataMediaPairId.hashCode());
        result = prime * result + ((sourceColumn == null) ? 0 : sourceColumn.hashCode());
        result = prime * result + ((targetColumn == null) ? 0 : targetColumn.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnPair other = (ColumnPair) obj;
        if (dataMediaPairId == null) {
            if (other.dataMediaPairId != null) return false;
        } else if (!dataMediaPairId.equals(other.dataMediaPairId)) return false;
        if (sourceColumn == null) {
            if (other.sourceColumn != null) return false;
        } else if (!sourceColumn.equals(other.sourceColumn)) return false;
        if (targetColumn == null) {
            if (other.targetColumn != null) return false;
        } else if (!targetColumn.equals(other.targetColumn)) return false;
        return true;
    }

}
