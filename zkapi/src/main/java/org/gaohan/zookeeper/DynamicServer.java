package org.gaohan.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * 服务器端向 Zookeeper 注册
 */
public class DynamicServer {

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
                // 打印所有node
                List<String> children = zk.getChildren("/", true);
                System.out.println(children);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    // 注册服务器
    public void registServer(String hostname) throws Exception {
        // 创建一个节点
        String parentNode = "/servers";
        String create = zk.create(parentNode + "/server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + "is online" + create);
    }

    // 业务功能
    public void business(String hostname) throws Exception {
        System.out.println(hostname + "is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception{
        // 1.获取zk连接
        DynamicServer server = new DynamicServer();
        server.getConnect();

        // 2.利用zk连接 注册服务器信息
        server.registServer(args[0]);

        // 3.启动业务功能
        server.business(args[0]);
    }
}
