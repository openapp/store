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

import java.io.InputStream;
import java.util.List;

public interface StoreServer {

	List<MapKeyValue> put(Field<?>... fields);

	List<MapKeyValue> remove(Field<?>... fields);

	Entry get(Field<?>... fields);

	Entries iterate(Entry first, Entry last, Field<?>... fields);

	Entries iterateNext(byte[] subsequent, Entry first, Entry last,
			Field<?>... fields);

	InputStream read(Blob blob);

	Blob write(InputStream stream, StreamEncoding inEncoding,
			StreamEncoding outEncoding);

}
