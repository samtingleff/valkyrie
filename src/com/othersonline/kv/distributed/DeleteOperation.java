package com.othersonline.kv.distributed;

public class DeleteOperation<V> extends AbstractOperation<V> {
	private static final long serialVersionUID = -918401158100309347L;

	public DeleteOperation(OperationCallback<V> callback, Node node, String key) {
		super(callback, node, key);
	}

	public OperationResult<V> call() throws Exception {
		try {
			store.delete(key);
			OperationResult<V> result = new DefaultOperationResult<V>(this,
					node, null);
			return result;
		} finally {
		}
	}

}
