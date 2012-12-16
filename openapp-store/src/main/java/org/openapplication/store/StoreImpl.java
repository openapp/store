package org.openapplication.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

public final class StoreImpl implements StoreServer {

	private final List<Definition> definitionList;

	private final Map<UUID, List<Definition>> definitionMap;

	private final MapProvider mapProvider;

	private List<ByteBuffer> blobBuffers;

	public StoreImpl(Definition... definitions) {
		this(new MapProviderImpl(), definitions);
	}

	public StoreImpl(MapProvider mapProvider, Definition... definitions) {
		this(mapProvider, Arrays.asList(definitions));
	}

	public StoreImpl(List<Definition> definitions) {
		this(new MapProviderImpl(), definitions);
	}

	public StoreImpl(MapProvider mapProvider, List<Definition> definitions) {
		definitionList = new ArrayList<Definition>();
		definitionMap = new HashMap<UUID, List<Definition>>();
		this.mapProvider = mapProvider;

		// System.out.println("Preparing maps");
		for (Definition definition : definitions) {
			UUID keyUuid = definition.toKeyUuid();

			definitionList.add(definition);

			if (!definitionMap.containsKey(keyUuid))
				definitionMap.put(keyUuid, new ArrayList<Definition>());
			definitionMap.get(keyUuid).add(definition);

			mapProvider.prepare(keyUuid);
		}
		// System.out.println("Preparing maps done");
	}

	@Override
	public List<MapKeyValue> put(Field<?>... fields) {
		Entry entry = new Entry(fields);
		List<MapKeyValue> ops = new ArrayList<MapKeyValue>();

		for (Definition definition : definitionList)
			if (entry.containsFieldValues(definition.getKeyFields())
					&& entry.containsFieldValues(definition.getValueFields()))
				ops.add(new MapKeyValue(definition.toKeyUuid(), definition,
						entry.getBytes(definition.getKeyFields()), entry
								.getBytes(definition.getValueFields())));

		for (MapKeyValue operation : ops)
			mapProvider.get(operation.map, operation.key, null).put(
					operation.key, operation.value);

		return ops;
	}

	@Override
	public List<MapKeyValue> remove(Field<?>... fields) {
		Entry entry = new Entry(fields);
		List<MapKeyValue> ops = new ArrayList<MapKeyValue>();

		for (Definition definition : definitionList)
			if (entry.containsFields(definition.getKeyFields()))
				ops.add(new MapKeyValue(definition.toKeyUuid(), definition,
						entry.getBytes(definition.getKeyFields()), null));

		for (MapKeyValue operation : ops)
			mapProvider.get(operation.map, operation.key, null).remove(
					operation.key);

		return ops;
	}

	@Override
	public Entry get(Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);
		Entry keyEntry = new Entry(fields);
		byte[] key = keyEntry.getBytes(fields);
		SortedMap<byte[], byte[]> storage = mapProvider.get(keyUuid, key, null);
		if (storage == null)
			return null;

		byte[] value = storage.get(key);
		if (value == null)
			return null;
		for (Definition definition : definitionMap.get(keyUuid))
			if (definition.matchesValue(value))
				return new Entry(keyEntry, new Entry(value,
						definition.getValueFields()));

		return keyEntry;
	}

	@Override
	public Entries iterate(Entry first, Entry last, Field<?>... fields) {
		return iterateNext(null, first, last, fields);
	}

	@Override
	public Entries iterateNext(byte[] subsequent, Entry first, Entry last,
			Field<?>... fields) {
		UUID keyUuid = Key.asUuid(fields);

		Entry entry = new Entry(fields);
		byte[] firstKey = null, lastKey = null;
		if (first != null)
			firstKey = new Entry(entry, first).getBytes(fields, false);
		if (last != null)
			lastKey = new Entry(entry, last).getBytes(fields, true);
		if (first == null && last == null) {
			firstKey = entry.getBytes(fields, false);
			lastKey = entry.getBytes(fields, true);
		} else if (first != null && last == null) {
			lastKey = firstKey;
			firstKey = entry.getBytes(fields, false);
		} else if (first == null && last != null) {
			firstKey = lastKey;
			lastKey = entry.getBytes(fields, true);
		}

		SortedMap<byte[], byte[]> storage;
		if (subsequent == null)
			storage = mapProvider.get(keyUuid, firstKey, null);
		else
			storage = mapProvider.get(keyUuid, subsequent, lastKey);
		if (storage == null)
			return null;

		SortedMap<byte[], byte[]> subMap = storage.subMap(firstKey, lastKey);
		List<Definition> definitions = definitionMap.get(keyUuid);

		return new MapEntryIterable(subMap.entrySet(), firstKey, lastKey,
				definitions);
	}

	// Must be >=1
	private static final int BLOB_MIN_REMAINING = 1;

	// Must be >=1
	private static final int BLOB_STREAM_BUFFER_SIZE = 50 * 1024;

	// Must be >=BLOB_STREAM_BUFFER_SIZE
	private static final int BLOB_VOLUME_SIZE = 1024 * 1024;

	@Override
	public InputStream read(final Blob blob) {
		final List<ByteBuffer> blobBuffers = this.blobBuffers;
		InputStream stream = new InputStream() {
			int index = 0;
			ByteBuffer buffer = null;

			private boolean prepare() {
				if (index >= blob.count())
					return true;
				long block = blob.block(index);
				int node = (int) Blob.asNode(block);
				int offset = (int) Blob.asOffset(block);
				int size = (int) blob.size(index);
				index++;
				buffer = blobBuffers.get(node).asReadOnlyBuffer();
				buffer.position(offset);
				buffer.limit(offset + size);
				return false;
			}

			@Override
			public int read() throws IOException {
				if (buffer == null && prepare())
					return -1;
				int read = buffer.get() & 0xff;
				if (!buffer.hasRemaining())
					buffer = null;
				return read;
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				if (b == null)
					throw new NullPointerException();
				else if (off < 0 || len < 0 || len > b.length - off)
					throw new IndexOutOfBoundsException();
				else if (len == 0)
					return 0;

				if (buffer == null && prepare())
					return -1;
				int read = Math.min(buffer.remaining(), len);
				buffer.get(b, 0, read);
				if (!buffer.hasRemaining())
					buffer = null;
				return read;
			}

			@Override
			public int available() throws IOException {
				if (buffer == null && prepare())
					return 0;
				return buffer.remaining();
			}
		};
		return stream;
	}

	@Override
	public synchronized Blob write(InputStream stream,
			StreamEncoding inEncoding, StreamEncoding outEncoding) {
		StreamEncoding encoding;
		// if (digest && inEncoding.equals(outEncoding)) {
		// if (!StreamEncoding.IDENTITY.equals(inEncoding))
		// // inEncoding == outEncoding, but we need a blob digest, so we
		// // have to decode anyway to get unencoded data for the digest
		// // (A possible optimization here would be to decode for purposes
		// // of calculating the digest, but use the undecoded stream
		// // directly for the output, rather than decoding and reencoding.
		// // However, the caller could instead itself filter the input to
		// // calculate the digest. Therefore, we document that this method
		// // will perform a decode and an encode if digest == true, even
		// // if inEncoding == outEncoding.)
		// try {
		// stream = inEncoding.createInputStream(stream);
		// } catch (IOException e) {
		// throw new RuntimeException(e);
		// }
		// encoding = outEncoding;
		// } else
		if (!inEncoding.equals(outEncoding)) {
			if (!StreamEncoding.IDENTITY.equals(inEncoding))
				// inEncoding != outEncoding, so we have to decode and reencode
				try {
					stream = inEncoding.createInputStream(stream);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			encoding = outEncoding;
		} else
			// inEncoding == outEncoding, // and we don't need a blob digest,
			// so let's pretend that we're doing an IDENTITY encode
			encoding = StreamEncoding.IDENTITY;

		MessageDigest streamDigest;
		MessageDigest blockDigest;
		try {
			streamDigest = MessageDigest.getInstance("SHA-256");
			blockDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("SHA-256 message digest unavailable");
		}

		OutputReader postEncoder = new OutputReader();
		OutputStream encoder;
		try {
			// Use the encoding decided above (which in the case of inEncoding
			// == outEncoding may be IDENTITY regardless of actual output
			// encoding)
			encoder = encoding.createOutputStream(postEncoder);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		Blob blob = new Blob();
		long blobSize = 0;
		for (;;) {
			Block block = writeBlock(stream, streamDigest, blockDigest,
					encoder, postEncoder);
			if (block.blockSize > 0)
				blob = blob.append(block.block, block.blockSize,
						merge((byte) 2, Arrays.copyOfRange( // 2: sha-256-128
								blockDigest.digest(), 0, 16)));
			blobSize += block.streamRead;
			if (block.isLast)
				return blob.append(blobSize,
						merge((byte) 1, streamDigest.digest()), // 1: sha-256
						outEncoding); // Actual output encoding
		}
	}

	private static class OutputReader extends OutputStream {
		private ByteArrayOutputStream bos = new ByteArrayOutputStream(
				BLOB_STREAM_BUFFER_SIZE) {
			@Override
			public synchronized byte[] toByteArray() {
				// We need direct access to buf, so it must not be cloned
				// (which would be done by super.toByteArray())
				return buf;
			}
		};

		private int pos = 0;

		@Override
		public void write(int b) throws IOException {
			bos.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			bos.write(b, off, len);
		}
	}

	private static byte[] merge(byte b1, byte[] a2) {
		byte[] result = new byte[1 + a2.length];
		result[0] = b1;
		System.arraycopy(a2, 0, result, 1, a2.length);
		return result;
	}

	private static class Block {
		final boolean isLast;
		final long block;
		final long blockSize;
		final long streamRead;

		public Block(boolean isLast, long block, long blockSize, long streamRead) {
			this.isLast = isLast;
			this.block = block;
			this.blockSize = blockSize;
			this.streamRead = streamRead;
		}
	}

	private Block writeBlock(InputStream stream, MessageDigest streamDigest,
			MessageDigest blockDigest, OutputStream encoder,
			OutputReader postEncoder) {
		ByteBuffer buffer = null;
		long node = 0;
		if (blobBuffers == null)
			blobBuffers = Collections
					.synchronizedList(new ArrayList<ByteBuffer>());
		else
			for (ByteBuffer b : blobBuffers)
				if (b.remaining() > BLOB_MIN_REMAINING
						+ (b.position() % 16 == 0 ? 0
								: (16 - b.position() % 16))) {
					buffer = b;
					break;
				} else
					node++;
		if (buffer == null)
			blobBuffers.add(buffer = ByteBuffer
					.allocateDirect(BLOB_VOLUME_SIZE));

		// 16 byte alignment
		if (buffer.position() % 16 != 0)
			for (int i = 16 - buffer.position() % 16; i > 0; i--)
				buffer.put((byte) 0);

		// Keep the starting offset
		long block = Blob.asBlock(node, buffer.position());

		// Read/write
		int blockSize = 0;
		int streamRead;
		int totalStreamRead = 0;
		try {
			byte[] bytes = null; // null: Optimization for
			if (encoder != postEncoder) // identity encoding
				bytes = new byte[BLOB_STREAM_BUFFER_SIZE];
			for (;;) {
				if (bytes == null) { // Optimization for identity encoding
					// Read directly into encoded buffer
					byte[] encoded = postEncoder.bos.toByteArray();
					streamRead = stream.read(encoded, 0, encoded.length);
					if (streamRead != -1) {
						// In order to update bos.size()
						// (Note that we write from the "encoded" array to the
						// same array and the same offset)
						postEncoder.write(encoded, 0, streamRead);

						// Block and stream digests are updated with the same
						// data as unencoded and encoded data are equivalent
						blockDigest.update(encoded, 0, streamRead);
						if (streamDigest != null)
							streamDigest.update(encoded, 0, streamRead);

						totalStreamRead += streamRead;
					}
				} else if (postEncoder.bos.size() > 0) {
					// We're on a new block, but still have encoded data, so
					// let's output that before encoding more
					streamRead = 0;
				} else if (-1 != (streamRead = stream.read(bytes, 0,
						bytes.length))) {
					// Feed the encoder
					encoder.write(bytes, 0, streamRead);

					// Update stream digest with unencoded data
					if (streamDigest != null)
						streamDigest.update(bytes, 0, streamRead);

					totalStreamRead += streamRead;
				} else
					encoder.close();

				while (postEncoder.bos.size() > 0 && buffer.hasRemaining()) {
					int encodedRead = Math.min(postEncoder.bos.size()
							- postEncoder.pos, buffer.remaining());
					// Output encoded data
					buffer.put(postEncoder.bos.toByteArray(), postEncoder.pos,
							encodedRead);

					// Feed block digest with encoded data
					blockDigest.update(postEncoder.bos.toByteArray(),
							postEncoder.pos, encodedRead);

					// Don't reset bos until we've read all of its contained
					// encoded data (pos keeps track of how much we've read)
					if ((postEncoder.pos += encodedRead) == postEncoder.bos
							.size()) {
						postEncoder.bos.reset();
						postEncoder.pos = 0;
					}

					blockSize += encodedRead;
				}

				if (streamRead == -1 || !buffer.hasRemaining())
					break;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return new Block(streamRead == -1 && postEncoder.bos.size() == 0,
				block, blockSize, totalStreamRead);
	}

}
