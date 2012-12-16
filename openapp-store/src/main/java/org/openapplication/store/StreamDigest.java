package org.openapplication.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class StreamDigest {

	private final MessageDigest digest;

	public StreamDigest() {
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("SHA-256 message digest unavailable");
		}
	}

	public byte[] hash() {
		return merge((byte) 1, digest.digest()); // 1: sha-256
	}

	private static byte[] merge(byte b1, byte[] a2) {
		byte[] result = new byte[1 + a2.length];
		result[0] = b1;
		System.arraycopy(a2, 0, result, 1, a2.length);
		return result;
	}

	public InputStream createInputStream(final InputStream in,
			final StreamEncoding encoding) throws IOException {
		if (StreamEncoding.IDENTITY.equals(encoding))
			return new DigestInputStream(in, digest);

		final ByteArrayOutputStream bos = new ByteArrayOutputStream() {
			@Override
			public byte[] toByteArray() {
				return buf;
			}
		};
		final InputStream digestIn = encoding
				.createInputStream(new InputStream() {
					@Override
					public int read() throws IOException {
						int read = in.read();
						if (read != -1)
							bos.write(read);
						return read;
					}
				});
		return new InputStream() {
			int pos = 0;

			@Override
			public int read() throws IOException {
				if (bos.size() > 0) {
					byte[] buf = bos.toByteArray();
					int read = buf[pos] & 0xff;
					if (pos + 1 == bos.size()) {
						bos.reset();
						pos = 0;
					} else
						pos++;
					return read;
				} else {
					int digestRead;
					while (bos.size() == 0)
						if (-1 != (digestRead = digestIn.read()))
							digest.update((byte) digestRead);
						else
							break;
					if (bos.size() == 0)
						return -1;
					byte[] buf = bos.toByteArray();
					int read = buf[0] & 0xff;
					if (1 == bos.size())
						bos.reset();
					else
						pos++;
					return read;
				}
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				if (bos.size() > 0) {
					byte[] buf = bos.toByteArray();
					int read = Math.min(len, bos.size() - pos);
					System.arraycopy(buf, pos, b, off, read);
					if (pos + read == bos.size()) {
						bos.reset();
						pos = 0;
					} else
						pos += read;
					return read;
				} else {
					int digestRead;
					while (bos.size() < len)
						if (-1 != (digestRead = digestIn.read()))
							digest.update((byte) digestRead);
						else
							break;
					if (bos.size() == 0)
						return -1;
					byte[] buf = bos.toByteArray();
					int read = Math.min(len, bos.size());
					System.arraycopy(buf, 0, b, off, read);
					if (read == bos.size())
						bos.reset();
					else
						pos += read;
					return read;
				}
			}
		};
	}

}
