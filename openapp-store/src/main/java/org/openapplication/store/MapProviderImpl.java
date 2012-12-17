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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MapProviderImpl implements MapProvider {

	private final Map<UUID, ConcurrentNavigableMap<byte[], byte[]>> storageMap;

	public MapProviderImpl() {
		storageMap = new HashMap<UUID, ConcurrentNavigableMap<byte[], byte[]>>();
	}

	public ConcurrentNavigableMap<byte[], byte[]> instantiate(UUID keyUuid) {
		return new ConcurrentSkipListMap<byte[], byte[]>(
				ByteArrayComparator.INSTANCE);
	}

	@Override
	public final void prepare(UUID keyUuid) {
		if (!storageMap.containsKey(keyUuid))
			storageMap.put(keyUuid, instantiate(keyUuid));
	}

	@Override
	public final ConcurrentNavigableMap<byte[], byte[]> get(UUID keyUuid,
			byte[] key, byte[] wantSubsequentUntil) {
		return wantSubsequentUntil == null ? storageMap.get(keyUuid) : null;
	}

}
