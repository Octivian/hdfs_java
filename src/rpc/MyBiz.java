package rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class MyBiz implements MyBizable{
	/* (non-Javadoc)
	 * @see rpc.MyBizable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name){
		return "hello"+name;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return MyBizable.versionID;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol,
			long clientVersion, int clientMethodsHash) throws IOException {
		return null;
	}
}
