package com.aerolush.lucene.store;

import com.aerospike.client.Value;
import com.spikeify.Spikeify;
import com.spikeify.Work;
import org.apache.lucene.store.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class AeroDirectory extends Directory {

	private final Spikeify sfy;
	private LockFactory lockFactory;

	public AeroDirectory(Spikeify sfy) {

		this.sfy = sfy;
		lockFactory = new AeroLockFactory(sfy);
	}

	@Override
	public String[] listAll() throws IOException {

		// lists only file names ... not whole entities
		List<Value> list = sfy.scanAll(File.class).keys();
		String[] output = new String[list.size()];

		// TODO: ... create keysAsString for Spikeify so we can use: output = list.toArray();
		for (int index = 0; index < list.size(); index++) {
			output[index] = list.get(index).toString();
		}

		return output;
	}

	@Override
	public void deleteFile(String name) throws IOException {

		File found = sfy.get(File.class).key(name).now();
		if (found != null) {
			sfy.delete(found).now();
		}

		throw new FileNotFoundException(name);
	}

	@Override
	public long fileLength(String name) throws IOException {

		File found = sfy.get(File.class).key(name).now();
		if (found != null) {
			return found.getLength();
		}

		throw new FileNotFoundException(name);
	}

	/**
	 * Create file
	 *
	 * @param name
	 * @param ioContext
	 * @return
	 * @throws IOException
	 */
	@Override
	public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {

		return new AeroIndexOutput(sfy, name);
	}

	/**
	 * Read file
	 *
	 * @param name
	 * @param ioContext
	 * @return
	 * @throws IOException
	 */
	@Override
	public IndexInput openInput(String name, IOContext ioContext) throws IOException {

		return new AeroIndexInput(sfy, name);
	}

	/**
	 * Ensure that any writes to these files are moved to
	 * stable storage.  Lucene uses this to properly commit
	 * changes to the index, to prevent a machine/OS crash
	 * from corrupting the index.
	 * <br>
	 * NOTE: Clients may call this method for same files over
	 * and over again, so some impls might optimize for that.
	 * For other impls the operation can be a noop, for various
	 * reasons.
	 */
	@Override
	public void sync(Collection<String> collection) throws IOException {
		// nothing to do ... all changes are always stored
	}

	@Override
	public void renameFile(String oldName, String newName) throws IOException {

		final File found = sfy.get(File.class).key(oldName).now();

		if (found != null) {
			File foundNew = sfy.get(File.class).key(newName).now();
			if (foundNew != null) {
				throw new IOException("File with: " + newName + ", already exists!");
			}

			// we can not rename the file key so we need to delete and create a new file instead
			// 1. we copy old file to new file ... and give it a new name
			final File newFile = new File(found, newName);

			sfy.transact(5, new Work<File>() {

				@Override
				public File run() {
					sfy.delete(found).now();
					sfy.create(newFile).now();
					return found;
				}
			});

			return;
		}

		throw new FileNotFoundException(oldName);
	}

	/**
	 * Lock's file fo operation
	 * TODO: Consider storing this info direclty into file entity? ...
	 *
	 * @param name
	 * @return
	 */
	@Override
	public Lock makeLock(String name) {

		return lockFactory.makeLock(this, name);
	}

	/**
	 * Closes the store.
	 **/
	@Override
	public void close() throws IOException {
		// nothing to do ...
	}
}
