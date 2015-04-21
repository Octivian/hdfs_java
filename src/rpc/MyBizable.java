package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol {
	public static final long versionID=123456L;
	public abstract String hello(String name);

}