package com.kappaware.ktail;

import java.util.List;
import java.util.Vector;

public class TopicDesc {
	private String name;
	private List<PartitionDesc> partitionDescs = new Vector<PartitionDesc>();
	private Long messageCount;
	private Long firstTimestamp;
	private Long lastTimestamp;

	public TopicDesc(String name) {
		super();
		this.name = name;
	}

	void addPartitionDesc(PartitionDesc p) {
		this.partitionDescs.add(p);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<PartitionDesc> getPartitionDescs() {
		return this.partitionDescs;
	}

	public void aggregate() {
		if (messageCount == null) {
			messageCount = 0L;
			for (PartitionDesc p : this.partitionDescs) {
				this.messageCount += (p.getLastOffset() - p.getFirstOffset());
				if (p.getFirstTimestamp() != null) {
					if (this.firstTimestamp == null || this.firstTimestamp > p.getFirstTimestamp()) {
						this.firstTimestamp = p.getFirstTimestamp();
					}
					if (p.getLastTimestamp() != null) {
						if (this.lastTimestamp == null || this.lastTimestamp < p.getLastTimestamp()) {
							this.lastTimestamp = p.getLastTimestamp();
						}
					}
				}
			}
		}
	}

	public String toString() {
		this.aggregate();
		if(this.messageCount == 0) {
			return String.format("Topic '%s': empty", this.name);
		} else {
			return String.format("Topic '%s': %d messages from %s up to %s", this.name, this.messageCount, Utils.printIsoDateTime(this.firstTimestamp), Utils.printIsoDateTime(this.lastTimestamp));
		}
	}
	
	static class PartitionDesc {
		private Long firstOffset;
		private Long lastOffset;
		private Long firstTimestamp;
		private Long lastTimestamp;

		public Long getFirstOffset() {
			return firstOffset;
		}

		public void setFirstOffset(Long firstOffset) {
			this.firstOffset = firstOffset;
		}

		public Long getLastOffset() {
			return lastOffset;
		}

		public void setLastOffset(Long lastOffset) {
			this.lastOffset = lastOffset;
		}

		public Long getFirstTimestamp() {
			return firstTimestamp;
		}

		public void setFirstTimestamp(Long firstTimestamp) {
			this.firstTimestamp = firstTimestamp;
		}

		public Long getLastTimestamp() {
			return lastTimestamp;
		}

		public void setLastTimestamp(Long lastTimestamp) {
			this.lastTimestamp = lastTimestamp;
		}

	}

}
