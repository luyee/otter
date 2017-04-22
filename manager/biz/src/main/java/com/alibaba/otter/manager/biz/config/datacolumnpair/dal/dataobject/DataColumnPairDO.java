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

package com.alibaba.otter.manager.biz.config.datacolumnpair.dal.dataobject;

import java.io.Serializable;
import java.util.Date;

/**
 * 类DataColumnPairDO.java的实现描述：TODO 类实现描述
 * 
 * @author simon 2012-4-20 下午4:09:38
 */
public class DataColumnPairDO implements Serializable {

    private static final long serialVersionUID = 194553152360180533L;
    private Long              id;
    private String            sourceColumnName;                      // 源字段
    private String            targetColumnName;                      // 目标字段
    private Long              dataMediaPairId;
    private Date              gmtCreate;
    private Date              gmtModified;
    private Integer 			  isPk=0;//是否主键(key)，1： 是 0：否 针对hbase,cassandra,es等kv结构的key
    private String 			  functionName;//字段值转换函数
    private Integer 			  sourceType;//数据来源类型 0-原始字段数据 1-表名 2-当前时间 3-当前用户

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

	public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    public String getTargetColumnName() {
        return targetColumnName;
    }

    public void setTargetColumnName(String targetColumnName) {
        this.targetColumnName = targetColumnName;
    }

    public Long getDataMediaPairId() {
        return dataMediaPairId;
    }

    public void setDataMediaPairId(Long dataMediaPairId) {
        this.dataMediaPairId = dataMediaPairId;
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

}
