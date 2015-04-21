package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyServer {
	public static final String SERVER_ADDRESS="localhost";
	public static final int SERVER_PORT=12345;
	public static void main(String[] args) throws Exception {
		/** Construct an RPC server.
		 * 构造一个RPC server
	     * @param instance the instance whose methods will be called
	     * 实例中的方法会被客户端调用
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * 绑定的地址用于监听链接到来
	     * @param port the port to listen for connections on
	     * 绑定的端口用于监听链接到来
	     * @param numHandlers the number of method handler threads to run
	     * @param verbose whether each call should be logged
	     */
//		final Server server = RPC.getServer(new MyBiz(),SERVER_ADDRESS, SERVER_PORT,new Configuration());
		RPC.Builder builder = new RPC.Builder(new Configuration());
		builder.setPort(SERVER_PORT);
		builder.setBindAddress(SERVER_ADDRESS);
		builder.setProtocol(MyBizable.class);
		builder.setInstance(new MyBiz());
		builder.build().start();
	}
}
