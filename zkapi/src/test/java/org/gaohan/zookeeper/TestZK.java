package org.gaohan.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestZK {

    //配置参数：url、连接事件
    private static final String connectString ="192.168.44.12:2181";
    private static final int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, event -> {
            // 收到事件通知后的回调函数（用户的业务逻辑）
            System.out.println(event.getType() + "--" + event.getPath() +"--"+event.getState().toString());
            // 再次启动监听
            try {
                List<String> children = zkClient.getChildren("/", true);
                System.out.println(children);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // 创建子节点
    @Test
    public void create() throws Exception {
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型 PERSISTENT：永久无编号节点
        String path = zkClient.create("/test", "testcreateznode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    //创建临时有序号节点
    @Test
    public void createTempNode() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/test/child1", "testcreatechildren".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("创建临时序号节点成功："+ nodeCreated);
        List<String> children = zkClient.getChildren("/", false);
        for (String child : children) {
            System.out.println(child);
        }
    }

    // 获取子节点 并监控节点变化
    @Test
    public void getChildren() throws Exception {

        List<String> children = zkClient.getChildren("/", true);

        System.out.println(children);

        // 延时阻塞-查看监听变化，process中需要再次监听才能一直监听子节点
        Thread.sleep(Long.MAX_VALUE);
    }

    // 节点数据修改
    @Test
    public void set() throws KeeperException, InterruptedException {
        //-1 表示忽略所有版本号
        zkClient.setData("/test","tt".getBytes(),-1);
    }

    // 判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test", false);
        System.out.println(stat == null ? "znode not exist" : stat);
    }

    //节点删除
    @Test
    public void delete() throws KeeperException, InterruptedException {
        //-1 表示忽略所有版本号
        zkClient.delete("/test",-1);
        System.out.println("已删除");
    }

}
