package org.apache.nifi.processors.nosql.util;

import com.mongodb.WriteConcern;


public enum WriteConcernEnum {
	ACKNOWLEDGED(WriteConcern.ACKNOWLEDGED),
	FSYNC_SAFE(WriteConcern.FSYNC_SAFE),
	FSYNCED(WriteConcern.FSYNCED),
	JOURNAL_SAFE(WriteConcern.JOURNAL_SAFE),
	JOURNALED(WriteConcern.JOURNALED),
	MAJORITY(WriteConcern.MAJORITY),
	NORMAL(WriteConcern.NORMAL),
	REPLICA_ACKNOWLEDGED(WriteConcern.REPLICA_ACKNOWLEDGED),
	REPLICAS_SAFE(WriteConcern.REPLICAS_SAFE),
	SAFE(WriteConcern.SAFE),
	UNACKNOWLEDGED(WriteConcern.UNACKNOWLEDGED);
	
	
	private final WriteConcern concern;
	
	private WriteConcernEnum(final WriteConcern concern) {
		this.concern = concern;
	}
	
	public WriteConcern getWriteConcern() {
		return concern;
	}
}
