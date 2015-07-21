package com.aerolush.lucene.store;

import com.spikeify.Spikeify;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Class for output to a file in a Directory. A random-access output stream. Used for all Lucene index output operations.
 */
public class AeroIndexOutput extends IndexOutput {

	private final File file;
	private final Spikeify sfy;

	private final CRC32 crcHash = new CRC32();
	private long pointer = 0;

	protected AeroIndexOutput(Spikeify sfy, String fileName) {

		// resourceDescription should be a non-null, opaque string describing this resource; it's returned from toString().
		super(fileName);

		file = new File(fileName);
		this.sfy = sfy;

		sfy.create(fileName, file).now();
	}

	/**
	 * Closes this stream to further operations.
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		// todo: add transaction
		file.close();
		sfy.update(file).now();
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
	 * The most primitive data type is an eight-bit byte. Files are accessed as sequences of bytes. All other data types are defined as sequences of bytes, so file formats are byte-order independent.
	 * @param bajt
	 * @throws IOException
	 */
	@Override
	public void writeByte(byte bajt) throws IOException {

		crcHash.update(bajt);
		pointer += 1;

		file.writeBytes(new byte[] {bajt}, 0, 1);
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

		file.writeBytes(bytes, offset, length);
	}

	/**Writes a single byte.
	 The most primitive data type is an eight-bit byte. Files are accessed as sequences of bytes. All other data types are defined as sequences of bytes, so file formats are byte-order independent.
	@Override
	public void writeByte(byte bajt) throws IOException {

		crcHash.update(bajt);
		pointer += 1;

		file.writeBytes(new byte[] { bajt }, 0, 1);

		sfy.update(file).now();
	}

	@Override
	public void writeBytes(byte[] bytes, int offset, int length) throws IOException {

		crcHash.update(bytes, offset, length);
		pointer += length;
		file.writeBytes(bytes, offset, length);

		sfy.update(file).now();
	}

	/**
	 * The size of data blocks, currently 16k (2^14), is determined by this
	 * constant.
	 */
/*	static public final int BLOCK_SHIFT = 14;
	static public final int BLOCK_LEN = 1 << BLOCK_SHIFT;
	static public final int BLOCK_MASK = BLOCK_LEN - 1;

	protected long position = 0L, length = 0L;
	protected AeroDirectory directory;

	protected Block block;
	protected File file;

	public AeroIndexOutput(AeroDirectory directory, String name, IOContext ioContext) {

		super();

		this.directory = directory;

		file = new File(directory, name, ioContext);
		block = new Block(file);
		length = file.getLength();

		seek(length);
		block.get(directory);

		directory.openFiles.add(this);
	}

	@Override
	public void close()
		throws IOException
	{
		flush();
		file.modify(directory, length, System.currentTimeMillis());

		directory.openFiles.remove(this);
	}

	@Override
	public void flush()
		throws IOException
	{
		if (length > 0)
			block.put(directory);
	}

	@Override
	public void writeByte(byte b)
		throws IOException
	{
		int blockPos = (int) (position++ & BLOCK_MASK);

		block.getData()[blockPos] = b;

		if (blockPos + 1 == BLOCK_LEN)
		{
			block.put(directory);
			block.seek(position);
			block.get(directory);
		}

		if (position > length)
			length = position;
	}

	@Override
	public void writeBytes(byte[] b, int offset, int len)
		throws IOException
	{
		int blockPos = (int) (position & BLOCK_MASK);

		while (blockPos + len >= BLOCK_LEN) {
			int blockLen = BLOCK_LEN - blockPos;

			System.arraycopy(b, offset, block.getData(), blockPos, blockLen);
			block.put(directory);

			len -= blockLen;
			offset += blockLen;
			position += blockLen;

			block.seek(position);
			block.get(directory);
			blockPos = 0;
		}

		if (len > 0)
		{
			System.arraycopy(b, offset, block.getData(), blockPos, len);
			position += len;
		}

		if (position > length)
			length = position;
	}

	@Override
	public long length()
		throws IOException
	{
		return length;
	}

	@Override
	public void seek(long pos)
		throws IOException
	{
		if (pos > length)
			throw new IOException("seeking past end of file");

		if ((pos >>> BLOCK_SHIFT) == (position >>> BLOCK_SHIFT))
			position = pos;
		else
		{
			block.put(directory);
			block.seek(pos);
			block.get(directory);
			position = pos;
		}
	}

	@Override
	public long getFilePointer()
	{
		return position;
	}*/
}
