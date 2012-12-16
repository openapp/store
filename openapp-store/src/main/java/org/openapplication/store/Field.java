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
