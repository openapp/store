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
