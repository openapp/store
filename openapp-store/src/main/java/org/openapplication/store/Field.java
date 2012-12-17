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
import java.util.UUID;

public interface Field<T> {

	static final int SIZE_NULL_TERMINATED = -1;

	static final int SIZE_VARIABLE_BYTE = -2;

	static final int SIZE_VARIABLE_SHORT = -3;

	static final int SIZE_VARIABLE_INTEGER = -4;

	Field<T> toField();

	UUID toUuid();

	String toUri();

	FieldValue<T> value(T value);

	FieldRange<T> range(T min, T max);

	int size();

	ByteBuffer toBytes(Object value);

	void put(ByteBuffer buffer, Object value);

	T get(ByteBuffer buffer);

	String toString(Object value);

}
