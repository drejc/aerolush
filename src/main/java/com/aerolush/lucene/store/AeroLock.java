package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import org.apache.lucene.store.Lock;

import java.io.IOException;

public class AeroLock extends Lock {

	private final Spikeify sfy;
	private final String name;

	public AeroLock(Spikeify sfy, String lockName) {

		this.sfy = sfy;
		this.name = lockName;
	}

	@Override
	public boolean obtain() throws IOException {

		FileLock lock = new FileLock(name);

		sfy.create(lock).now();

		return true;
	}

	@Override
	public void close() throws IOException {

		FileLock found = sfy.get(FileLock.class).key(name).now();
		if (found != null) {
			sfy.delete(found).now();
		}
	}

	@Override
	public boolean isLocked() throws IOException {

		FileLock found = sfy.get(FileLock.class).key(name).now();
		return found != null;
	}
}
