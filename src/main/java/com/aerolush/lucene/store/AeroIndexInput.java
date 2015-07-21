package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import org.apache.lucene.store.IndexInput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AeroIndexInput extends IndexInput {

	// private final Spikeify sfy;
	private final File file;
	private final long offset;
	private long pointer;

	protected AeroIndexInput(Spikeify sfy, String fileName) throws FileNotFoundException {

		// resourceDescription should be a non-null, opaque string describing this resource; it's returned from toString().
		super(fileName);

		file = sfy.get(File.class).key(fileName).now();
		if (file == null) {
			throw new FileNotFoundException(fileName);
		}

		offset = 0;
	}

	protected AeroIndexInput(File file, long offset) {

		super(file.getFileName());

		this.file = file;
		this.offset = offset;
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

		return new AeroIndexInput(file, offset) {
			@Override
			public long length() {

				return length;
			}
		};

	}

	/**
	 * Reads and returns a single byte.
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
	 * @param bytes the array to read bytes into
	 * @param offset the offset in the array to start storing bytes
	 * @param len the number of bytes to read
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

		ByteBuffer seg = file.getSegment(getCurrentSegmentNumber());

		int position = seg.position();
		int offset = getCurrentSegmentOffset();

		seg.position(position + offset);
		return seg;
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

	/*private final CassandraFile m_file;
	private long m_pointer;
	private long m_offset = 0;

	CassandraIndexInput(CassandraFile file) {
		this(file, 0);
	}

	CassandraIndexInput(CassandraFile file, long offset) {
		super(file.getResourceDescription());
		m_file = Preconditions.checkNotNull(file, "file argument");
		m_offset = offset;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFilePointer() {
		return m_pointer;
	}

	@Override
	public void seek(long pos) throws IOException {
		m_pointer = pos;
	}

	@Override
	public long length() {
		return m_file.getLength();
	}

	@Override
	public IndexInput slice(String sliceDescription, long offset, final long length) throws IOException {
		return new CassandraIndexInput(m_file, m_pointer + offset) {
			@Override
			public long length() {
				return length;
			}
		};
	}

	@Override
	public byte readByte() throws IOException {
		byte b = getCurrentSegment().get();
		m_pointer += 1;
		return b;
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		ByteBuffer buf = getCurrentSegment();

		if (len <= buf.remaining()) {
			buf.get(b, offset, len);
			m_pointer += len;
		}
		else {
			int remaining = buf.remaining();
			buf.get(b, offset, remaining);
			m_pointer += remaining;
			readBytes(b, offset + remaining, len - remaining);
		}

	}

	private ByteBuffer getCurrentSegment() throws IOException {
		ByteBuffer seg = m_file.getSegment(getCurrentSegmentNumber());
		seg.position(seg.position() + getCurrentSegmentOffset());
		return seg;
	}

	private int getCurrentSegmentOffset() {
		return (int)(getPosition() % CassandraFile.SEGMENT_SIZE);
	}

	private long getCurrentSegmentNumber() {
		return (getPosition() / CassandraFile.SEGMENT_SIZE) + 1;
	}

	private long getPosition() {
		return m_pointer + m_offset;
	}*/
}
