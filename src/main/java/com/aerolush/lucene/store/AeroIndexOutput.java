package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import com.spikeify.Work;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Class for output to a file in a Directory.
 * A random-access output stream. Used for all Lucene index output operations.
 */
public class AeroIndexOutput extends IndexOutput {

	private final File file;
	private final Spikeify sfy;

	private final CRC32 crcHash = new CRC32();
	private long pointer = 0;

	protected AeroIndexOutput(Spikeify spikeify, String fileName) {

		// resourceDescription should be a non-null, opaque string describing this resource; it's returned from toString().
		super(fileName);

		file = new File(fileName);

		sfy = spikeify;
		sfy.create(fileName, file).now();
	}

	/**
	 * Closes this stream to further operations.
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		// todo: add transaction
		file.close(sfy);

		sfy.transact(5, new Work<File>() {
			@Override
			public File run() {

				File original = sfy.get(File.class).key(file.getFileName()).now();
				// take data from file
				original.copy(file);

				sfy.update(original).now();
				return original;
			}
		});
	}

	/**
	 * Returns the current position in this file, where the next write will occur.
	 * @return
	 */
	@Override
	public long getFilePointer() {

		return pointer;
	}

	/**
	 * Returns the current checksum of bytes written so far
	 * @return
	 * @throws IOException
	 */
	@Override
	public long getChecksum() throws IOException {

		return crcHash.getValue();
	}

	/**
	 * Writes a single byte.
	 * The most primitive data type is an eight-bit byte. Files are accessed as sequences of bytes.
	 * All other data types are defined as sequences of bytes, so file formats are byte-order independent.
	 * @param bajt
	 * @throws IOException
	 */
	@Override
	public void writeByte(byte bajt) throws IOException {

		crcHash.update(bajt);
		pointer += 1;

		file.writeBytes(new byte[] {bajt}, 0, 1, sfy);
	}

	/**
	 * Writes an array of bytes.
	 * @param bytes
	 * @param offset
	 * @param length
	 * @throws IOException
	 */
	@Override
	public void writeBytes(byte[] bytes, int offset, int length) throws IOException {

		crcHash.update(bytes, offset, length);
		pointer += length;

		file.writeBytes(bytes, offset, length, sfy);
	}
}
