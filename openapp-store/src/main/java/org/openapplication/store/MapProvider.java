package org.openapplication.store;

import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;

public interface MapProvider {

	void prepare(UUID keyUuid);

	ConcurrentNavigableMap<byte[], byte[]> get(UUID keyUuid, byte[] key,
			byte[] wantSubsequentUntil);

}