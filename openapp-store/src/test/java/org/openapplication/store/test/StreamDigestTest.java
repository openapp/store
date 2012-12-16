package org.openapplication.store.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openapplication.store.StreamDigest;
import org.openapplication.store.StreamEncoding;


public class StreamDigestTest {

	private static final Charset CHARSET = Charset.forName("UTF-8");

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	java.security.SecureRandom rnd = new java.security.SecureRandom();

	@Test
	public void testHash() throws IOException, NoSuchAlgorithmException {
		byte[] helloWorld = "Hello World!Hello World!Hello World!Hello World!Hello World!"
				.getBytes(CHARSET);
		// rnd.nextBytes(helloWorld);
		byte[] helloWorldActualHash;
		{
			MessageDigest referenceDigest = MessageDigest
					.getInstance("SHA-256");
			byte[] hash = referenceDigest.digest(helloWorld);
			helloWorldActualHash = new byte[hash.length + 1];
			helloWorldActualHash[0] = 1; // 1: sha-256
			System.arraycopy(hash, 0, helloWorldActualHash, 1, hash.length);
		}

		// IDENTITY

		InputStream source = new ByteArrayInputStream(helloWorld);
		// InputStream source = StreamEncoding.IDENTITY
		// .createInputStream(new ByteArrayInputStream(helloWorld));

		StreamDigest digest = new StreamDigest();
		InputStream stream = digest.createInputStream(source,
				StreamEncoding.IDENTITY);

		assertNotNull(stream);
		int read, i = 0;
		while ((read = stream.read()) != -1)
			assertEquals(helloWorld[i++] & 0xff, read);
		assertEquals(-1, read);
		assertEquals(helloWorld.length, i);

		assertArrayEquals(helloWorldActualHash, digest.hash());

		// GZIP - int read()

		ByteArrayOutputStream gzipOut = new ByteArrayOutputStream();
		OutputStream gzipEncoder = StreamEncoding.GZIP
				.createOutputStream(gzipOut);
		gzipEncoder.write(helloWorld);
		gzipEncoder.close();
		byte[] helloWorldEncoded = gzipOut.toByteArray();

		source = new ByteArrayInputStream(helloWorldEncoded);

		// digest = new StreamDigest(); // should have no effect
		stream = digest.createInputStream(source, StreamEncoding.GZIP);

		assertNotNull(stream);
		i = 0;
		while ((read = stream.read()) != -1)
			assertEquals(helloWorldEncoded[i++] & 0xff, read);
		assertEquals(-1, read);
		assertEquals(helloWorldEncoded.length, i);

		assertArrayEquals(helloWorldActualHash, digest.hash());

		// GZIP - int read(byte[] b, int off, int len)

		source = new ByteArrayInputStream(helloWorldEncoded);

		digest = new StreamDigest(); // should have no effect
		stream = digest.createInputStream(source, StreamEncoding.GZIP);

		byte[] bytesRead = new byte[10];
		ByteArrayOutputStream allBytesReadOut = new ByteArrayOutputStream();
		while (-1 != (read = stream.read(bytesRead)))
			allBytesReadOut.write(bytesRead, 0, read);
		byte[] allBytesRead = allBytesReadOut.toByteArray();

		assertArrayEquals(helloWorldEncoded, allBytesRead);

		assertArrayEquals(helloWorldActualHash, digest.hash());
	}

}
