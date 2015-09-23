package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import org.apache.lucene.store.IndexInput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

public class AeroIndexInput extends IndexInput {

	// private final Spikeify sfy;
	private final File file;
	private final long offset;
	private final Spikeify sfy;
	private final List<FileSegment> segments;
	private long pointer;

	private HashMap<Long, ByteBuffer> cachedSegment = new HashMap<>();

	protected AeroIndexInput(Spikeify spikeify, String fileName) throws FileNotFoundException {

		// resourceDescription should be a non-null, opaque string describing this resource; it's returned from toString().
		super(fileName);

		file = spikeify.get(File.class).key(fileName).now();
		if (file == null) {
			throw new FileNotFoundException(fileName);
		}

		segments = spikeify.query(FileSegment.class).filter("name", file.segmentsName).now().toList();

		sfy = spikeify;
		offset = 0;
	}

	protected AeroIndexInput(Spikeify spikeify, File file, long offset) {

		super(file.getFileName());

		this.file = file;
		this.offset = offset;

		segments = spikeify.query(FileSegment.class).filter("name", file.segmentsName).now().toList();

		sfy = spikeify;
	}

	/**
	 * Closes the stream to further operations.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {

		pointer = 0;
	}

	/**
	 * Returns the current position in this file, where the next read will occur.
	 *
	 * @return
	 */
	@Override
	public long getFilePointer() {

		return pointer;
	}

	/**
	 * Sets current position in this file, where the next read will occur.
	 *
	 * @param position
	 * @throws IOException
	 */
	@Override
	public void seek(long position) throws IOException {

		pointer = position;
	}

	/**
	 * The number of bytes in the file.
	 *
	 * @return
	 */
	@Override
	public long length() {

		return file.getLength();
	}

	/**
	 * Creates a slice of this index input, with the given description, offset, and length. The slice is seeked to the beginning.
	 *
	 * @param sliceDescription
	 * @param offset
	 * @param length
	 * @return
	 * @throws IOException
	 */
	@Override
	public IndexInput slice(String sliceDescription, long offset, final long length) throws IOException {

		return new AeroIndexInput(sfy, file, offset) {
			@Override
			public long length() {

				return length;
			}
		};

	}

	/**
	 * Reads and returns a single byte.
	 *
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte readByte() throws IOException {

		byte bajt = getCurrentSegment().get();
		pointer += 1;
		return bajt;
	}

	/**
	 * Reads a specified number of bytes into an array at the specified offset.
	 *
	 * @param bytes  the array to read bytes into
	 * @param offset the offset in the array to start storing bytes
	 * @param len    the number of bytes to read
	 * @throws IOException
	 */
	@Override
	public void readBytes(byte[] bytes, int offset, int len) throws IOException {

		ByteBuffer buf = getCurrentSegment();

		if (len <= buf.remaining()) {
			buf.get(bytes, offset, len);
			pointer += len;
		}
		else {
			int remaining = buf.remaining();
			buf.get(bytes, offset, remaining);
			pointer += remaining;
			readBytes(bytes, offset + remaining, len - remaining);
		}
	}

	private ByteBuffer getCurrentSegment() throws IOException {

		long segmentNumber = getCurrentSegmentNumber();

		// try finding segment as loaded
		ByteBuffer seg = findSegment(segmentNumber);
		if (seg == null) { // not found ... go get it from file
			seg = file.getSegment(segmentNumber, sfy);
		}

		cachedSegment.put(segmentNumber, seg);

		int position = seg.position();
		int offset = getCurrentSegmentOffset();

		seg.position(position + offset);
		return seg;
	}

	private ByteBuffer findSegment(long segmentNumber) {

		String id = FileSegment.generateId(file.segmentsName, segmentNumber);
		for (FileSegment segment : segments) {
			if (segment.id.equals(id)) {
				ByteBuffer buffer = ByteBuffer.allocate(segment.data.length);
				buffer.put(segment.data);
				buffer.flip();
				return buffer;
			}
		}

		return null;
	}

	private int getCurrentSegmentOffset() {

		return (int) (getPosition() % File.SEGMENT_SIZE);
	}

	private long getCurrentSegmentNumber() {

		return (getPosition() / File.SEGMENT_SIZE) + 1;
	}

	private long getPosition() {

		return pointer + offset;
	}
}
