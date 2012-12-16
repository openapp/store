package org.openapplication.store;

import java.util.UUID;

public class MapKeyValue {

	public final UUID map;

	public final Definition definition;

	public final byte[] key;

	public final byte[] value;

	public MapKeyValue(UUID map, Definition definition, byte[] key, byte[] value) {
		this.map = map;
		this.definition = definition;
		this.key = key;
		this.value = value;
	}

}