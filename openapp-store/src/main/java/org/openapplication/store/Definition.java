package org.openapplication.store;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class Definition {

	private final String name;

	private final Field<?>[] keyFields;

	private final Field<?>[] valueFields;

	private final UUID keyUuid;

	public Definition(Field<?>[] keyFields, Field<?>[] valueFields) {
		this(null, keyFields, valueFields);
	}

	public Definition(String name, Field<?>[] keyFields, Field<?>[] valueFields) {
		this.name = name;
		this.keyFields = keyFields.clone();
		this.valueFields = valueFields.clone();
		this.keyUuid = Key.asUuid(keyFields);
	}

	public Field<?>[] getKeyFields() {
		return keyFields.clone();
	}

	public Field<?>[] getValueFields() {
		return valueFields.clone();
	}

	public UUID toKeyUuid() {
		return keyUuid;
	}

	public boolean matchesValue(byte[] value) {
		int position = 0;

		for (int i = 0, size; i < valueFields.length; i++)
			if ((size = valueFields[i].size()) >= 0) {
				int nextPosition = position + size;
				if (nextPosition > value.length)
					return false;
				if (valueFields[i] instanceof FieldValue<?>) {
					Object expected = ((FieldValue<?>) valueFields[i])
							.toValue();
					Object actual = valueFields[i].get(ByteBuffer.wrap(value,
							position, size).asReadOnlyBuffer());
					if (expected != actual
							&& (expected == null || !expected.equals(actual)))
						return false;
				}
				position = nextPosition;
			} else if (i == valueFields.length - 1) {
				position = value.length;
			} else {
				switch (size) {
				case Field.SIZE_NULL_TERMINATED:
					if (position == value.length)
						return false;
					while (value[position] != 0)
						if (++position == value.length)
							break;
					position += 1;
					break;
				case Field.SIZE_VARIABLE_BYTE:
					position += ByteBuffer.wrap(value, position, 1).get() & 0xff;
					position += 1;
					break;
				case Field.SIZE_VARIABLE_SHORT:
					position += ByteBuffer.wrap(value, position, 2).getShort() & 0xffff;
					position += 2;
					break;
				case Field.SIZE_VARIABLE_INTEGER:
					position += ByteBuffer.wrap(value, position, 4).getInt() & 0xffffffff;
					position += 4;
					break;
				}
				if (position >= value.length)
					return false;
			}

		return position == value.length;
	}

	@Override
	public String toString() {
		return name != null ? name : super.toString();
	}

}
