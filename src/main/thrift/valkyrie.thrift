namespace java com.rubiconproject.oss.kv.gen

const i32 DEFAULT_PORT = 9010

struct GetResult {
  1: bool exists,
  2: binary data
}

exception KeyValueStoreIOException {
}

exception KeyValueStoreException {
}

service KeyValueService {

  bool exists(1:string key)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  GetResult getValue(1:string key)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  map<string, GetResult> getBulk(1:list<string> keys)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  void setValue(1:string key, 2:binary data)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  void deleteValue(1:string key)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException)
}

