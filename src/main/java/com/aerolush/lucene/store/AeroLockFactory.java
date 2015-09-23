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

	/**
	 * Return a new Lock instance identified by lockName.
	 * @param directory parent directory
	 * @param lockName name of the lock to be created.
	 */
	@Override
	public Lock makeLock(Directory directory, String lockName) {

		return new AeroLock(sfy, lockName);
	}
}
