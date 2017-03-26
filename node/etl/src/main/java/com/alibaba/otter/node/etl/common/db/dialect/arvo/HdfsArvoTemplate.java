package com.alibaba.otter.node.etl.common.db.dialect.arvo;

import java.util.List;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventData;

public class HdfsArvoTemplate implements NoSqlTemplate {

	@Override
	public List<EventData> batchEventDatas(List<EventData> events) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData insertEventData(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData updateEventData(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData deleteEventData(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData createTable(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData alterTable(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData eraseTable(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData truncateTable(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventData renameTable(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void distory() throws ConnClosedException {
		// TODO Auto-generated method stub
		
	}

}
