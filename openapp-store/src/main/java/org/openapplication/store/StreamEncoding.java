package org.openapplication.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum StreamEncoding {

	IDENTITY((byte) 0), GZIP((byte) 1);

	private final byte id;

	private StreamEncoding(byte id) {
		this.id = id;
	}

	public byte toByte() {
		return id;
	}

	public static StreamEncoding fromByte(byte id) {
		switch (id) {
		case 0:
			return IDENTITY;
		case 1:
			return GZIP;
		}
		throw new IllegalArgumentException();
	}

	public InputStream createInputStream(InputStream in) throws IOException {
		switch (this) {
		case IDENTITY:
			return in;
		case GZIP:
			return new GZIPInputStream(in);
		}
		throw new RuntimeException();
	}

	public OutputStream createOutputStream(OutputStream out) throws IOException {
		switch (this) {
		case IDENTITY:
			return out;
		case GZIP:
			return new GZIPOutputStream(out);
		}
		throw new RuntimeException();
	}

}
