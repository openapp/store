/**
 * Copyright 2012 Erik Isaksson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openapplication.store;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public final class Blob {

	private final long[] block;

	private final long[] blockSize;

	private final byte[][] blockHash;

	private final long blobSize;

	private final byte[] blobHash;

	private final StreamEncoding encoding;

	public Blob() {
		this(new long[] {}, new long[] {}, new byte[][] {}, 0, null, null);
	}

	private Blob(long[] block, long[] blockSize, byte[][] blockHash,
			long blobSize, byte[] blobHash, StreamEncoding encoding) {
		this.block = block;
		this.blockSize = blockSize;
		this.blockHash = blockHash;
		this.blobSize = blobSize;
		this.blobHash = blobHash;
		this.encoding = encoding;
	}

	public int count() {
		return block.length;
	}

	public long block(int index) {
		return block[index];
	}

	public long size() {
		return blobSize;
	}

	public long size(int index) {
		return blockSize[index];
	}

	public byte[] hash() {
		return blobHash;
	}

	public byte[] hash(int index) {
		return blockHash == null ? null : blockHash[index];
	}

	public StreamEncoding encoding() {
		return encoding;
	}

	public Blob append(long block, long blockSize, byte[] blockHash) {
		return new Blob(merge(this.block, block), merge(this.blockSize,
				blockSize), merge(this.blockHash, blockHash), blobSize,
				blobHash, encoding);
	}

	public Blob append(long blobSize, byte[] blobHash, StreamEncoding encoding) {
		return new Blob(block, blockSize, blockHash, blobSize, blobHash,
				encoding);
	}

	private static long[] merge(long[] a1, long l2) {
		long[] result = Arrays.copyOf(a1, a1.length + 1);
		result[a1.length] = l2;
		return result;
	}

	private static <T> T[] merge(T[] a1, T t2) {
		T[] result = Arrays.copyOf(a1, a1.length + 1);
		result[a1.length] = t2;
		return result;
	}

	public static long asBlock(long node, long offset) {
		if (node < 0 || node > 0xfFfffL)
			throw new IllegalArgumentException("Invalid node");
		if (offset < 0 || offset > 0xfffFfffFfffL || (offset & 0xf) != 0)
			throw new IllegalArgumentException("Invalid offset");
		return (node << 40) | (offset >> 4);
	}

	public static long asNode(long block) {
		return block >> 40;
	}

	public static long asOffset(long block) {
		return (block & 0xffFfffFfffL) << 4;
	}

	public static final Field<Blob> BLOB = new BlobField(
			UUID.fromString("0c8412de-26c6-4910-a2c7-42b49d428413"));

	public static final class BlobField implements Field<Blob> {

		private final UUID uuid;

		public BlobField(UUID uuid) {
			this.uuid = uuid;
		}

		@Override
		public Field<Blob> toField() {
			return this;
		}

		@Override
		public UUID toUuid() {
			return uuid;
		}

		@Override
		public String toUri() {
			return "http://purl.org/openapp/fields/blob.blob";
		}

		@Override
		public FieldValue<Blob> value(Blob value) {
			return new FieldValue<Blob>(this, value);
		}

		@Override
		public FieldRange<Blob> range(Blob min, Blob max) {
			return new FieldRange<Blob>(this, min, max);
		}

		@Override
		public int size() {
			return Field.SIZE_VARIABLE_INTEGER;
		}

		@Override
		public ByteBuffer toBytes(Object value) {
			ByteBuffer buffer = ByteBuffer
					.allocate(9 + ((Blob) value).count() * 12);
			put(buffer, value);
			return buffer;
		}

		@Override
		public void put(ByteBuffer buffer, Object value) {
			Blob blob = (Blob) value;
			buffer.putLong(blob.blobSize);
			buffer.put(blob.encoding.toByte());
			for (int i = 0; i < blob.count(); i++) {
				buffer.putLong(blob.block[i]);
				buffer.putInt((int) blob.blockSize[i]);
			}
		}

		@Override
		public Blob get(ByteBuffer buffer) {
			long blobSize = buffer.getLong();
			StreamEncoding encoding = StreamEncoding.fromByte(buffer.get());
			int count = buffer.remaining() / 12;
			long[] block = new long[count];
			long[] blockSize = new long[count];
			for (int i = 0; i < count; i++) {
				block[i] = buffer.getLong();
				blockSize[i] = buffer.getInt() & 0xFfffFfff;
			}
			return new Blob(block, blockSize, null, blobSize, null, encoding);
		}

		@Override
		public String toString(Object value) {
			return ((Blob) value).size() + " bytes";
		}

	}

}
