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

public final class FieldValue<T> implements Field<T> {

	private final Field<T> field;

	private final T value;

	private final int size;

	public FieldValue(Field<T> field, T value) {
		this.field = field;
		this.value = value;
		this.size = field.size();
	}

	public T toValue() {
		return value;
	}

	@Override
	public Field<T> toField() {
		return field;
	}

	@Override
	public UUID toUuid() {
		return field.toUuid();
	}

	@Override
	public String toUri() {
		return field.toUri();
	}

	@Override
	public FieldValue<T> value(T value) {
		return field.value(value);
	}

	@Override
	public FieldRange<T> range(T min, T max) {
		return field.range(min, max);
	}

	@Override
	public int size() {
		return size; // return field.size();
	}

	@Override
	public ByteBuffer toBytes(Object value) {
		return field.toBytes(value);
	}

	@Override
	public void put(ByteBuffer buffer, Object value) {
		field.put(buffer, value);
	}

	@Override
	public T get(ByteBuffer buffer) {
		return field.get(buffer);
	}

	@Override
	public String toString(Object value) {
		return field.toString(value);
	}

	@Override
	public String toString() {
		return "value:" + field.toString();
	}

}
