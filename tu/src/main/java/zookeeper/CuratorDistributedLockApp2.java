package zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorDistributedLockApp2 {

    private String rootNode = "/locks";

    private String connectString = "192.168.44.12:2181";

    private int connectTimeout = 2000;

    private int sessionTimeout = 2000;

    public static void main(String[] args) {
        new CuratorDistributedLockApp2().test();
    }

    private void test(){

        //创建分布式锁1
        final InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), rootNode);

        //创建分布式锁2
        final InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), rootNode);

        new Thread(new Runnable() {

            @Override
            public void run() {
                //获取锁对象
                try {
                    lock1.acquire();
                    System.out.println("线程 1 获取锁");
                    // 测试锁重入
                    lock1.acquire();
                    System.out.println("线程 1 再次获取锁");
                    Thread.sleep(5 * 1000);
                    lock1.release();
                    System.out.println("线程 1 释放锁");
                    lock1.release();
                    System.out.println("线程 1 再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {

            @Override
            public void run() {
                //获取锁对象
                try {
                    lock2.acquire();
                    System.out.println("线程 2 获取锁");
                    // 测试锁重入
                    lock2.acquire();
                    System.out.println("线程 2 再次获取锁");
                    Thread.sleep(5 * 1000);
                    lock2.release();
                    System.out.println("线程 2 释放锁");
                    lock2.release();
                    System.out.println("线程 2 再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }


    public CuratorFramework getCuratorFramework() {
        //重试策略 初试时间3s 重试3次
        RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);

        // 通过工厂创建Curator
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .connectionTimeoutMs(connectTimeout)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(policy).build();

        //开启连接
        client.start();
        System.out.println("zookeeper初始化完成");

        return client;
    }
}
