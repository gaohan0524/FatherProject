package zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 测试ZooKeeper原生API实现的分布式锁
 * 启动10个线程，同时向zookeeper注册节点
 */
public class DistributedLockTest {
    public static void main(String[] args) throws IOException {
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        countDownLatch.await();
                        // 10线程并发执行以下代码
                        DistributedLockApp distributedLock = new DistributedLockApp();
                        distributedLock.lock();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "Thread-" + i).start();
            countDownLatch.countDown();
        }
        System.in.read();
    }
}
