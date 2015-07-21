package com.aerolush.lucene.store;

import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Ignore;
import com.spikeify.annotations.UserKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * File is split in segments of SEGMENT_SIZE
 */
public class File {

	static int SEGMENT_SIZE = 4096;

	@Generation
	public Integer generation;

	@UserKey
	public String name;	// file name ...

	protected long length; // total length

	protected long timeModified;

	/*@Ignore
	private int numOfSegments;
*/
	@Ignore
	private long segmentPointer = 1;

	@Ignore
	// temp buffer
	private ByteBuffer m_buffer = ByteBuffer.allocate(SEGMENT_SIZE);

	private Map<Long, byte[]> data = new HashMap<>();

	public File(String fileName) {
		this();
		name = fileName;
	}

	public File() {
		timeModified = System.currentTimeMillis();
		length = 0;
	}

	public File(File file, String fileName) {
		this(fileName);
		length = file.getLength();
		data = file.data;
	}

	public long getLength() {
		return length;
	}

	/*public long getNumberOfSegments() {
		return data.size();
	}*/

	/**
	 * get specific segment of file
	 * @param segmentNo
	 * @return
	 * @throws IOException
	 **/
	ByteBuffer getSegment(long segmentNo) throws IOException {

		byte[] segment = data.get(segmentNo);
		if (segment == null) {
			throw new IOException("Cannot read segment: " + segmentNo);
		}

		ByteBuffer buffer = ByteBuffer.allocate(segment.length);
		buffer.put(segment);
		buffer.flip();
		return buffer;
	}

	/**
	 * Fill up segments in buffer ...
	 * @param b
	 * @param offset
	 * @param writeLength
	 * @throws IOException
	 */
	void writeBytes(byte[] b, int offset, int writeLength) throws IOException {

		int remaining = m_buffer.remaining();

		if (remaining > writeLength) {
			length += writeLength;
			m_buffer.put(b, offset, writeLength);
		}
		else {
			length += remaining;
			m_buffer.put(b, offset, remaining);

			syncBuffer();

			writeBytes(b, offset + remaining, writeLength - remaining);
		}
	}

	private void syncBuffer() {

		m_buffer.flip();

		// store into data
		data.put(segmentPointer, m_buffer.array());

		// TODO: storing into database ... but should we do this here or in directory?

		segmentPointer += 1;

		m_buffer.clear();
	}

	public String getFileName() {

		return name;
	}

	void close() {

		// Flush whats left in the buffer
		syncBuffer();
	}
}
