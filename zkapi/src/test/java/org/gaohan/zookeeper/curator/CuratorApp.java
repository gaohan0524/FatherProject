package org.gaohan.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 使用推荐的方式(CuratorCache)实现
 * 原生API：递归删除
 * 	/ruozedata  连带/ruozedata一起删除
 * 			    保留/ruozedata不删，只删下面的
 */
public class CuratorApp {

    private final String namespace = "curator";
    private CuratorFramework client = null;
    private final String path = "/ruozedata/node1";

    @Before
    public void setUp() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        String connectionString = "192.168.217.10:2181";

        client = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .sessionTimeoutMs(2000)
                .connectionTimeoutMs(2000)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();
    }

    // 创建节点
    @Test
    public void create() throws Exception {

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, "createSuccess".getBytes());
    }

    // 1.递归删除，连带/ruozedata
    @Test
    public void deleteContainsDir() throws Exception {
        client.delete()
                .guaranteed()
                .deletingChildrenIfNeeded()
                .forPath(path);
    }

    // 2.递归删除，只删/ruozedata下面的
    @Test
    public void deleteNotContainsDir() throws Exception {
        List<String> childList = client.getChildren().forPath("/ruozedata");
        for (String child : childList) {
            client.delete()
                    .guaranteed()
                    .deletingChildrenIfNeeded()
                    .forPath("/ruozedata/" +child);
            System.out.println("已删除：" + child);
        }
    }

}
