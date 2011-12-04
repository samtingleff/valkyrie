namespace java com.othersonline.kv.gen

const i32 DEFAULT_PORT = 9010

struct GetResult {
  1: bool exists,
  2: binary data
}

struct GetRequest {
  1: string key,
  2: i32 maxHops
}

struct SetRequest {
  1: string key,
  2: i32 maxHops,
  3: binary data
}

struct DeleteRequest {
  1: string key,
  2: i32 maxHops
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

  // server-to-server get request
  GetResult getS2S(1:GetRequest request)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  map<string, GetResult> getBulk(1:list<string> keys)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  void setValue(1:string key, 2:binary data)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  // server-to-server set request
  void setS2S(1:SetRequest request)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException),

  void deleteValue(1:string key)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException)

  // server-to-server delete request
  void deleteS2S(1:DeleteRequest request)
    throws (1:KeyValueStoreIOException ioException, 2:KeyValueStoreException keyValueStoreException)
}

