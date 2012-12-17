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

import java.util.Comparator;

public final class ByteArrayComparator implements Comparator<byte[]> {

	public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

	private ByteArrayComparator() {
	}

	@Override
	public int compare(byte[] o1, byte[] o2) {
		int o1Length = o1.length, o2Length = o2.length;
		int minLength = (o1Length <= o2Length) ? o1Length : o2Length;

		for (int i = 0, result; i < minLength; i++)
			if (0 != (result = (o1[i] & 0xff) - (o2[i] & 0xff)))
				return result;

		return o1Length - o2Length;
	}

}
