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
