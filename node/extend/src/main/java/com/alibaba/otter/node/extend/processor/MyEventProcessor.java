package com.alibaba.otter.node.extend.processor;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;

public class MyEventProcessor extends AbstractEventProcessor {
	
	private static final Logger logger             = LoggerFactory.getLogger(MyEventProcessor.class);
    public boolean process(EventData eventData) {
    	List<EventColumn> columns = eventData.getColumns();
    	if((eventData.getTableName()!=null&&eventData.getTableName().equalsIgnoreCase("repayment_plan"))){
    		logger.warn("skip EventData table:"+ eventData.getTableName());
    		/* 判断下是否包含字段settlement_bill_type, withhold_type*/
//    		if(!containSpecsColumns(columns, "settlement_bill_type")){
//    		
//
//    		}
    		return false;
    	}
  
        return true;
    }

    private JSONObject doColumn(EventColumn column) {
        JSONObject obj = new JSONObject();
        obj.put("name", column.getColumnName());
        obj.put("update", column.isUpdate());
        obj.put("key", column.isKey());
        if (column.getColumnType() != Types.BLOB && column.getColumnType() != Types.CLOB) {
            obj.put("value", column.getColumnValue());
        } else {
            obj.put("value", "");
        }
        return obj;
    }
    
    /* 判断下是否包含字段settlement_bill_type, withhold_type*/
    private boolean containSpecsColumns(List<EventColumn> columns,String columnName){
    	boolean ret =false;
    	for (EventColumn eventColumn : columns) {
    		if(columnName.equalsIgnoreCase(eventColumn.getColumnName())){
    			ret =true;
    		}
		}
		return ret;
    	
    }
}