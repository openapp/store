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
package org.openapplication.store.disk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.openapplication.encoding.Binary;
import org.openapplication.store.ByteArrayComparator;
import org.openapplication.store.MapProvider;

public class DiskMapProvider implements MapProvider {

	private final Map<UUID, SortedMap<byte[], ConcurrentNavigableMap<byte[], byte[]>>> storageMap;

	private final Map<UUID, SortedMap<byte[], File>> fileMap;

	private final Map<UUID, SortedMap<byte[], RandomAccessFile>> accessMap;

	private final File directory;

	public DiskMapProvider(File directory) {
		storageMap = new HashMap<UUID, SortedMap<byte[], ConcurrentNavigableMap<byte[], byte[]>>>();
		fileMap = new HashMap<UUID, SortedMap<byte[], File>>();
		accessMap = new HashMap<UUID, SortedMap<byte[], RandomAccessFile>>();
		this.directory = directory;
	}

	public ConcurrentNavigableMap<byte[], byte[]> instantiate() {
		return new ConcurrentSkipListMap<byte[], byte[]>(
				ByteArrayComparator.INSTANCE);
	}

	@Override
	public final void prepare(UUID keyUuid) {
		if (!storageMap.containsKey(keyUuid)) {
			storageMap
					.put(keyUuid,
							new TreeMap<byte[], ConcurrentNavigableMap<byte[], byte[]>>(
									ByteArrayComparator.INSTANCE));

			accessMap.put(keyUuid, new TreeMap<byte[], RandomAccessFile>(
					ByteArrayComparator.INSTANCE));

			SortedMap<byte[], File> files = new TreeMap<byte[], File>(
					ByteArrayComparator.INSTANCE);
			fileMap.put(keyUuid, files);

			File mapDir = new File(directory, keyUuid.toString());
			if (!mapDir.exists()) {
				mapDir.mkdirs();
				try {
					new File(mapDir, Binary.asString(new byte[] { 0 }))
							.createNewFile();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			for (File mapFile : mapDir.listFiles())
				files.put(Binary.asByteArray(mapFile.getName()), mapFile);
		}
	}

	@Override
	public final ConcurrentNavigableMap<byte[], byte[]> get(UUID keyUuid,
			byte[] key, byte[] wantSubsequentUntil) {
		SortedMap<byte[], ConcurrentNavigableMap<byte[], byte[]>> maps = storageMap
				.get(keyUuid);
		if (maps == null)
			return null;

		// TODO: Handling for multiple regions
		if (wantSubsequentUntil != null)
			return null;
		byte[] mapKey = new byte[] { 0 };

		ConcurrentNavigableMap<byte[], byte[]> map = maps.get(mapKey);
		if (map == null) {
			System.out.println("Reading map");

			File file = fileMap.get(keyUuid).get(mapKey);

			RandomAccessFile access;
			try {
				access = new RandomAccessFile(file, "rw");
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
			accessMap.get(keyUuid).put(mapKey, access);

			map = instantiate();
			maps.put(mapKey, map);

			try {
				while (access.getFilePointer() < access.length()) {
					int readLength = access.readShort();
					byte[] readKey = new byte[readLength];
					access.readFully(readKey);

					readLength = access.readShort();
					byte[] readValue = new byte[readLength];
					access.readFully(readValue);

					if (readKey.length > 0)
						map.put(readKey, readValue);
					else
						map.remove(readValue);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			System.out.println("Reading map done");
		}

		return map;
	}

	public RandomAccessFile getFile(UUID keyUuid, byte[] key) {
		// TODO: Handling for multiple regions
		return accessMap.get(keyUuid).get(new byte[] { 0 });
	}

}
