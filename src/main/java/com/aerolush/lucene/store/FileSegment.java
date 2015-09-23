package com.aerolush.lucene.store;

import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

public class FileSegment {

	@UserKey
	protected String id; //

	@Indexed
	protected String name;

	protected byte[] data; // holding data

	protected FileSegment() {
	}

	public FileSegment(String fileSegmentName, long segmentNumber, byte[] data) {
		this();

		this.id = generateId(fileSegmentName, segmentNumber);
		this.name = fileSegmentName;
		this.data = data;
	}

	static String generateId(String fileSegmentName, long segmentNumber) {

		return fileSegmentName + "::" + segmentNumber;
	}
}
