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

package com.alibaba.otter.manager.web.home.module.action;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.math.NumberUtils;

import com.alibaba.citrus.service.form.CustomErrors;
import com.alibaba.citrus.service.form.Group;
import com.alibaba.citrus.turbine.Navigator;
import com.alibaba.citrus.turbine.dataresolver.FormField;
import com.alibaba.citrus.turbine.dataresolver.FormGroup;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.otter.manager.biz.config.datacolumnpair.DataColumnPairService;
import com.alibaba.otter.manager.biz.config.datamediapair.DataMediaPairService;
import com.alibaba.otter.shared.common.model.config.data.Column;
import com.alibaba.otter.shared.common.model.config.data.ColumnPair;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;

public class ColumnPairAction extends AbstractAction {

    @Resource(name = "dataColumnPairService")
    private DataColumnPairService dataColumnPairService;

    @Resource(name = "dataMediaPairService")
    private DataMediaPairService  dataMediaPairService;

    /**
     * 添加Channel
     * 
     * @param channelInfo
     * @param channelParameterInfo
     * @throws Exception
     */
    public void doSave(@Param("dataMediaPairId") Long dataMediaPairId, @Param("submitKey") String submitKey,
                       @Param("channelId") Long channelId, @Param("pipelineId") Long pipelineId,
                       @Param("sourceMediaId") Long sourceMediaId, @Param("targetMediaId") Long targetMediaId,
                       @FormGroup("columnPairInfo") Group columnPairInfo,
                       @FormField(name = "formColumnPairError", group = "columnPairInfo") CustomErrors err,
                       Navigator nav) throws Exception {
        String[] sourceColumns = columnPairInfo.getField("dltTarget_l").getStringValues();
        String[] targetColumns = columnPairInfo.getField("dltTarget_r").getStringValues();
        List<String> sourceColumnNames = new ArrayList<String>();
        List<String> targetColumnNames = new ArrayList<String>();
        for (String sourceColumn : sourceColumns) {
            sourceColumnNames.add(org.apache.commons.lang.StringUtils.split(sourceColumn,":")[0]);
        }

        for (String targetColumn : targetColumns) {
            targetColumnNames.add(targetColumn);
        }

        DataMedia targetMedia = dataMediaPairService.findById(dataMediaPairId).getTarget();

        if (!(targetMedia.getSource().getType().isNapoli() || targetMedia.getSource().getType().isElasticSearch() 
        		|| targetMedia.getSource().getType().isHbase()||targetMedia.getSource().getType().isKafka()||targetMedia.getSource().getType().isHDFSArvo()) && sourceColumnNames.size() != targetColumnNames.size()) {
            err.setMessage("invalidColumnPair");
            return;
        }
        List<ColumnPair> columnPairsInDb = dataColumnPairService.listByDataMediaPairId(dataMediaPairId);
        List<ColumnPair> columnPairsTemp = new ArrayList<ColumnPair>();
        List<String> columnPairsNameSource = new ArrayList<String>();
        List<String> columnPairsNameTarget = new ArrayList<String>();
        List<ColumnPair> columnPairs = new ArrayList<ColumnPair>();

        if (targetMedia.getSource().getType().isNapoli()) {
            for (ColumnPair columnPair : columnPairsInDb) {
                for (String sourceColumnName : sourceColumnNames) {
                    if (StringUtils.isEquals(columnPair.getSourceColumn().getName(), sourceColumnName)) {
                        columnPairsTemp.add(columnPair);
                        columnPairsNameSource.add(sourceColumnName);
                    }
                }
            }
            // 要从数据库中删除这些columnPair
            columnPairsInDb.removeAll(columnPairsTemp);
            sourceColumnNames.removeAll(columnPairsNameSource);

            for (String columnName : sourceColumnNames) {
                ColumnPair columnPair = new ColumnPair();
                columnPair.setSourceColumn(new Column(columnName));
                columnPair.setDataMediaPairId(dataMediaPairId);
                columnPairs.add(columnPair);
            }
        } else if (targetMedia.getSource().getType().isMysql() || targetMedia.getSource().getType().isOracle()
        		|| targetMedia.getSource().getType().isGreenplum()||targetMedia.getSource().getType().isCassandra()) {
            for (ColumnPair columnPair : columnPairsInDb) {
                int i = 0;
                for (String sourceColumnName : sourceColumnNames) {
                    if (StringUtils.isEquals(columnPair.getSourceColumn().getName(), sourceColumnName)
                        && StringUtils.isEquals(columnPair.getTargetColumn().getName(), targetColumnNames.get(i))) {
                        columnPairsTemp.add(columnPair);
                        columnPairsNameSource.add(sourceColumnName);
                        columnPairsNameTarget.add(targetColumnNames.get(i));
                    }
                    i++;
                }
            }
            // 要从数据库中删除这些columnPair
            columnPairsInDb.removeAll(columnPairsTemp);
            sourceColumnNames.removeAll(columnPairsNameSource);
            targetColumnNames.removeAll(columnPairsNameTarget);

            int i = 0;
            for (String column :sourceColumns ) {
            	String[] cs=org.apache.commons.lang.StringUtils.split(column,":");
	            for (String columnName : sourceColumnNames) {
	            	if (org.apache.commons.lang.StringUtils.equalsIgnoreCase(cs[0], columnName)){
		                ColumnPair columnPair = new ColumnPair();
		                columnPair.setSourceColumn(new Column(columnName));
		                if (cs.length==3){
                        	columnPair.setIsPk(NumberUtils.toInt(cs[1]));
                        	columnPair.setFunctionName(cs[2]);
                        }
		                columnPair.setTargetColumn(new Column(targetColumnNames.get(i)));
		                columnPair.setDataMediaPairId(dataMediaPairId);
		                columnPairs.add(columnPair);
		                i++;
		                break;
	            	}
	            }
            }
        }else{
        	//获取已经配置在数据库中的字段映射
        	for (ColumnPair columnPair : columnPairsInDb) {
                int i = 0;
                for (String sourceColumnName : sourceColumnNames) {
                    if (StringUtils.isEquals(columnPair.getSourceColumn().getName(), sourceColumnName)
                        && StringUtils.isEquals(columnPair.getTargetColumn().getName(), targetColumnNames.get(i))) {
                        columnPairsTemp.add(columnPair);
                        columnPairsNameSource.add(sourceColumnName);
                        columnPairsNameTarget.add(targetColumnNames.get(i));
                    }
                    i++;
                }
            }
            // 要从数据库中删除这些已经配置的columnPair
            columnPairsInDb.removeAll(columnPairsTemp);
            sourceColumnNames.removeAll(columnPairsNameSource);
            targetColumnNames.removeAll(columnPairsNameTarget);

            int i = 0;
            for (String column :sourceColumns ) {
            	String[] cs=org.apache.commons.lang.StringUtils.split(column,":");
            	for (String columnName :sourceColumnNames){
            		if (org.apache.commons.lang.StringUtils.equalsIgnoreCase(cs[0], columnName)){
            			ColumnPair columnPair = new ColumnPair();
                        //设置主键函数信息
                        columnPair.setSourceColumn(new Column(columnName));
                        if (cs.length==3){
                        	columnPair.setIsPk(NumberUtils.toInt(cs[1]));
                        	columnPair.setFunctionName(cs[2]);
                        }
                        //非关系数据库和cassanddra,没有目标方的字段自动建立同名字段
                        if (targetColumnNames.size()>=i+1){
                        	columnPair.setTargetColumn(new Column(targetColumnNames.get(i)));
                        }else{
                        	columnPair.setTargetColumn(new Column(columnName));
                        }
                        columnPair.setDataMediaPairId(dataMediaPairId);
                        columnPairs.add(columnPair);
                        i++;
                        break;
            		}
            	}
            }
        }

        for (ColumnPair columnPair : columnPairsInDb) {
            dataColumnPairService.remove(columnPair.getId());
        }

        dataColumnPairService.createBatch(columnPairs);

        if (submitKey.equals("保存")) {
            nav.redirectToLocation("dataMediaPairList.htm?pipelineId=" + pipelineId);
        } else if (submitKey.equals("下一步")) {
            nav.redirectToLocation("addColumnPairGroup.htm?dataMediaPairId=" + dataMediaPairId + "&channelId="
                                   + channelId + "&pipelineId=" + pipelineId + "&sourceMediaId=" + sourceMediaId
                                   + "&targetMediaId=" + targetMediaId);
        }
    }
}
