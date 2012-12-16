package org.openapplication.store;

import java.io.InputStream;
import java.util.List;

public interface Store {

	List<MapKeyValue> put(Field<?>... fields);

	List<MapKeyValue> remove(Field<?>... fields);

	Entry get(Field<?>... fields);

	Entries iterate(Field<?>... fields);

	Entries iterate(Entry first, Entry last, Field<?>... fields);

	InputStream read(Blob blob);

	Blob write(InputStream stream, StreamEncoding inEncoding,
			StreamEncoding outEncoding);

}
