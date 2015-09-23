package com.aerolush.lucene.store;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;

/**
 * Entity holding info about locked files
 */
public class FileLock {

	@Generation
	public Integer generation;

	@UserKey
	public String name;	// file name ...

	public long lockTime;

	// todo ... add expiration period

	private FileLock() {}

	public FileLock(String fileName) {

		name = fileName;
		lockTime = System.currentTimeMillis();
	}
}
