package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class AeroLockFactory extends LockFactory {

	private final Spikeify sfy;

	public AeroLockFactory(Spikeify sfy) {
		this.sfy = sfy;
	}

	@Override
	public Lock makeLock(Directory directory, String lockName) {

		return new AeroLock(sfy, lockName);
	}
}
