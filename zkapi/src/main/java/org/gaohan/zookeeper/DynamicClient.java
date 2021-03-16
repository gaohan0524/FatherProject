package org.gaohan.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * 客户端
 */
public class DynamicClient {

    private static final String connectString = "cdh01:2181,cdh02:2181,cdh03:2181";
    private static final int sessionTimeout = 2000;
    private ZooKeeper zk = null;

    // 创建到zk的客户端连接
    public void getConnect() throws Exception {
        zk = new ZooKeeper(connectString, sessionTimeout, event -> {
            // 收到事件通知后的回调函数（用户的业务逻辑）
            System.out.println(event.getType() + "--" + event.getPath() +"--"+event.getState().toString());
            // 再次启动监听
            try {
                getServerList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void getServerList() throws Exception{
        String parentNode = "/servers";
        // 1.获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);

        // 2.存储服务器信息列表
        ArrayList<Object> servers = new ArrayList<>();

        // 3.遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        // 4.打印服务器列表信息
        System.out.println(servers);
    }

    // 业务功能
    public void business() throws Exception {
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        // 1.获取zk连接
        DynamicClient client = new DynamicClient();
        client.getConnect();

        // 2.获取servers的子节点信息，从中获取服务器信息列表
        client.getServerList();

        // 3.业务进程启动
        client.business();
    }

}
