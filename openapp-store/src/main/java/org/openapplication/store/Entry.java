package org.openapplication.store;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;

public final class Entry {

	private final Map<Field<?>, Object> values;

	public Entry(Field<?>... fields) {
		values = new IdentityHashMap<Field<?>, Object>(fields.length);
		for (Field<?> field : fields)
			if (field instanceof FieldValue<?>) {
				FieldValue<?> fieldValue = (FieldValue<?>) field;
				values.put(fieldValue.toField(), fieldValue.toValue());
			}
	}

	public Entry(Entry e1, Entry e2) {
		values = new IdentityHashMap<Field<?>, Object>(e1.values.size()
				+ e2.values.size());
		values.putAll(e1.values);
		values.putAll(e2.values);
	}

	public Entry(byte[] bytes, Field<?>... fields) {
		values = new IdentityHashMap<Field<?>, Object>(fields.length);
		for (int i = 0, position = 0; i < fields.length; i++) {
			Field<?> field = fields[i];

			int fieldSize = field.size(), valueStart = position, valueSize = fieldSize;
			if (fieldSize < 0)
				if (i == fields.length - 1)
					fieldSize = (valueSize = bytes.length - position);
				else
					switch (fieldSize) {
					case Field.SIZE_NULL_TERMINATED:
						int test = position;
						while (bytes[test] != 0)
							test++;
						valueSize = test - position;
						fieldSize = valueSize + 1;
						break;
					case Field.SIZE_VARIABLE_BYTE:
						valueSize = ByteBuffer.wrap(bytes, position, 1).get() & 0xff;
						valueStart = position + 1;
						fieldSize = valueSize + 1;
						break;
					case Field.SIZE_VARIABLE_SHORT:
						valueSize = ByteBuffer.wrap(bytes, position, 2)
								.getShort() & 0xffff;
						valueStart = position + 2;
						fieldSize = valueSize + 2;
						break;
					case Field.SIZE_VARIABLE_INTEGER:
						valueSize = ByteBuffer.wrap(bytes, position, 4)
								.getInt() & 0xffffffff;
						valueStart = position + 4;
						fieldSize = valueSize + 4;
						break;
					}

			values.put(field.toField(), field.get(ByteBuffer.wrap(bytes,
					valueStart, valueSize).asReadOnlyBuffer()));
			position += fieldSize;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T get(Field<T> field) {
		return (T) values.get(field.toField());
	}

	public boolean containsFields(Field<?>... fields) {
		for (Field<?> field : fields)
			if (!values.containsKey(field.toField()))
				return false;
		return true;
	}

	public boolean containsFieldValues(Field<?>... fields) {
		for (Field<?> field : fields)
			if (values.containsKey(field.toField()))
				if (field instanceof FieldValue<?>)
					if (((FieldValue<?>) field).toValue() == null)
						if (values.get(field.toField()) != null)
							return false;
						else
							continue;
					else if (!((FieldValue<?>) field).toValue().equals(
							values.get(field.toField())))
						return false;
					else
						continue;
				else
					continue;
			else
				return false;
		return true;
	}

	public byte[] getBytes(Field<?>... fields) {
		return getBytes(fields, false, false);
	}

	public byte[] getBytes(Field<?>[] fields, boolean stop) {
		return getBytes(fields, true, stop);
	}

	private byte[] getBytes(Field<?>[] fields, boolean range, boolean stop) {
		int varCount = 0;
		for (Field<?> field : fields)
			if (field.size() < 0)
				varCount++;

		ByteBuffer[] vars = new ByteBuffer[varCount];
		for (int i = 0, varIndex = 0; i < fields.length; i++)
			if (fields[i].size() < 0)
				vars[varIndex++] = (ByteBuffer) fields[i].toBytes(
						values.get(fields[i].toField())).rewind();

		int length = 0;
		for (int i = 0, size, varIndex = 0; i < fields.length; i++)
			if ((size = fields[i].size()) >= 0)
				length += size;
			else if (i == fields.length - 1)
				length += vars[varIndex++].limit();
			else
				switch (size) {
				case Field.SIZE_NULL_TERMINATED:
				case Field.SIZE_VARIABLE_BYTE:
					length += vars[varIndex++].limit() + 1;
					break;
				case Field.SIZE_VARIABLE_SHORT:
					length += vars[varIndex++].limit() + 2;
					break;
				case Field.SIZE_VARIABLE_INTEGER:
					length += vars[varIndex++].limit() + 4;
					break;
				}
		if (range && stop)
			length++;

		byte[] bytes = new byte[length];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		for (int i = 0, size, varIndex = 0; i < fields.length; i++)
			if (range && !values.containsKey(fields[i].toField()))
				if (!stop && fields[i] instanceof FieldRange<?>
						&& ((FieldRange<?>) fields[i]).getMin() != null)
					fields[i].put(buffer, ((FieldRange<?>) fields[i]).getMin());
				else if (stop && fields[i] instanceof FieldRange<?>
						&& ((FieldRange<?>) fields[i]).getMax() != null)
					fields[i].put(buffer, ((FieldRange<?>) fields[i]).getMax());
				else
					for (int j = fields[i].size(); j > 0; j--)
						buffer.put(stop ? (byte) -1 : 0);
			else if ((size = fields[i].size()) >= 0)
				fields[i].put(buffer, values.get(fields[i].toField()));
			else if (i == fields.length - 1)
				buffer.put(vars[varIndex++]);
			else
				switch (size) {
				case Field.SIZE_NULL_TERMINATED:
					ByteBuffer src = vars[varIndex++];
					byte srcByte;
					int srcLength = src.limit();
					for (int j = 0; j < srcLength; j++)
						if (0 != (srcByte = src.get()))
							buffer.put(srcByte);
						else
							throw new IllegalArgumentException(
									"Null character within null-terminated value of field: "
											+ fields[i]);
					buffer.put((byte) 0);
					break;
				case Field.SIZE_VARIABLE_BYTE:
					buffer.put((byte) vars[varIndex].limit());
					buffer.put(vars[varIndex++]);
					break;
				case Field.SIZE_VARIABLE_SHORT:
					buffer.putShort((short) vars[varIndex].limit());
					buffer.put(vars[varIndex++]);
					break;
				case Field.SIZE_VARIABLE_INTEGER:
					buffer.putInt(vars[varIndex].limit());
					buffer.put(vars[varIndex++]);
					break;
				}
		if (range && stop)
			buffer.put((byte) 0);

		return bytes;
	}

}
