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

package com.alibaba.otter.node.etl.select.selector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.extend.communication.CanalConfigClient;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.DataSourcing;
import com.alibaba.otter.node.common.config.ConfigClientService;
import com.alibaba.otter.node.etl.OtterContextLocator;
import com.alibaba.otter.node.etl.select.selector.canal.CanalEmbedSelector;
import com.alibaba.otter.node.etl.select.selector.kafka.KafkaSelector;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;

/**
 * 获取对应的selector
 * 
 * @author jianghang 2012-8-1 上午10:25:06
 * @version 4.1.0
 */
public class OtterSelectorFactory {
    private static final Logger     logger           = LoggerFactory.getLogger(OtterSelectorFactory.class);

	private ConfigClientService configClientService;
	
	
	private CanalConfigClient canalConfigClient;
	
	
    public OtterSelector getSelector(Long pipelineId) {
    	
    	Pipeline pipeline= configClientService.findPipeline(pipelineId);
    	
    	String  destination = pipeline.getParameters().getDestinationName();
    	Canal canal = canalConfigClient.findCanal(destination);
    	//String brokerServer =  canal.getCanalParameter().getDbAddresses();
    	List<List<DataSourcing>> groupDbAddresses =  canal.getCanalParameter().getGroupDbAddresses();
    	
    	if(canal.getCanalParameter().getSourcingType().isKafka()){
    		String  topicList = canal.getCanalParameter().getKafkaTopicList();
    		DataSourcing dataSourcing = groupDbAddresses.get(0).get(0);
    		String host = dataSourcing.getDbAddress().getHostString();
    		int port = dataSourcing.getDbAddress().getPort();
    		KafkaSelector selector = new KafkaSelector(host+":"+port,destination,pipeline,topicList);
            OtterContextLocator.autowire(selector);
    		return selector;
    	}else{
    		CanalEmbedSelector selector = new CanalEmbedSelector(pipelineId);
            OtterContextLocator.autowire(selector);
            return selector;
    	}
        
        
    }
	public ConfigClientService getConfigClientService() {
		return configClientService;
	}
	public void setConfigClientService(ConfigClientService configClientService) {
		this.configClientService = configClientService;
	}
	
    public void setCanalConfigClient(CanalConfigClient canalConfigClient) {
        this.canalConfigClient = canalConfigClient;
    }
	
	
    
    
    
    

}
