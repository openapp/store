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

import java.security.MessageDigest;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.openapplication.encoding.Id;
import org.openapplication.encoding.Id.Name;

public class Key {

	public static final UUID NS_KEY = UUID
			.fromString("0328f4af-e43e-41f0-a727-a9fc10c21504");

	private static Map<Key, UUID> keyUuidCache = new ConcurrentHashMap<Key, UUID>();

	public static UUID asUuid(final Field<?>... key) {
		Key keyKey = new Key(key);
		UUID uuid = keyUuidCache.get(keyKey);
		if (uuid != null)
			return uuid;
		keyUuidCache.put(keyKey, uuid = Id.asUuid(NS_KEY, new Name() {
			@Override
			public void update(MessageDigest digest) {
				for (Field<?> field : key)
					digest.update(Id.asByteArray(field.toUuid()));
			}
		}));
		return uuid;
	}

	private final Field<?>[] key;

	private final int hashCode;

	public Key(Field<?>... key) {
		this.key = new Field<?>[key.length];
		int result = 1;
		for (int i = 0; i < key.length; i++) {
			Field<?> field = key[i];
			this.key[i] = field = field.toField();
			// if (field instanceof FieldValue<?>)
			// this.key[i] = field = ((FieldValue<?>) key[i]).toField();
			result = 31 * result + field.hashCode();
		}
		this.hashCode = result;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		Field<?>[] key2 = ((Key) obj).key;
		int length = key.length;
		if (key2.length != length)
			return false;
		for (int i = 0; i < length; i++)
			if (key[i] != key2[i])
				return false;
		return true;
	}

}
