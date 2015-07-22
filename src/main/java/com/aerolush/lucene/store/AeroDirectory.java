package com.aerolush.lucene.store;

import com.spikeify.ResultSet;
import com.spikeify.Spikeify;
import org.apache.lucene.store.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class AeroDirectory extends Directory {

	private final Spikeify sfy;
	private LockFactory lockFactory;

	public AeroDirectory(Spikeify sfy) {

		this.sfy = sfy;
		lockFactory = new AeroLockFactory(sfy);
	}

	public Spikeify getSfy() {
		return sfy;
	}

	@Override
	public String[] listAll() throws IOException {

		ResultSet<File> list = sfy.query(File.class).indexName("fileName").now();
		List<String> output = new ArrayList<>();

		Iterator<File> it = list.iterator();
		while (it.hasNext()) {
			File file = it.next();
			output.add(file.name);
		}

		return output.toArray(new String[output.size()]);
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
	 * @param name
	 * @param ioContext
	 * @return
	 * @throws IOException
	 */
	@Override
	public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {

		return new AeroIndexOutput(getSfy(), name);
	}

	/**
	 * Read file
	 * @param name
	 * @param ioContext
	 * @return
	 * @throws IOException
	 */
	@Override
	public IndexInput openInput(String name, IOContext ioContext) throws IOException {

		return new AeroIndexInput(sfy, name);
	}

	@Override
	public void sync(Collection<String> collection) throws IOException {
		// TODO: ... applicable ? ...probably not
	}

	@Override
	public void renameFile(String oldName, String newName) throws IOException {

		File found = sfy.get(File.class).key(oldName).now();

		if (found != null) {
			File foundNew = sfy.get(File.class).key(newName).now();
			if (foundNew != null) {
				throw new IOException("File with: " + newName + ", already exists!");
			}


			File newFile = new File(found, newName);

			sfy.transact(5, () -> {

				sfy.delete(found).now();
				sfy.create(newFile).now();
				return found;
			});

			return;
		}

		throw new FileNotFoundException(oldName);
	}

	@Override
	public Lock makeLock(String name) {

		return lockFactory.makeLock(this, name);
	}

	@Override
	public void close() throws IOException {
		// nothing to do ... or is there something?

	}
}
