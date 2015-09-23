package com.aerolush.lucene.store;

import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

public class FileSegment {


	@UserKey
	protected String id; //

	/*
	 * consists of file name and segment number
	 */
	@Indexed
	protected String name;

	@Indexed
	protected String fileName;

	protected long number;

	protected byte[] data; // holding data

	protected FileSegment() {
	}

	public FileSegment(String fileName, long segmentNumber, byte[] data) {
		this();

		this.id = IdGenerator.generateKey();
		this.fileName = fileName;
		this.number = segmentNumber;

		this.name = getSegmentName(fileName, segmentNumber);
		this.data = data;
	}

	static String getSegmentName(String fileName, long segmentNumber) {

		return fileName + "::" + segmentNumber;
	}
}
