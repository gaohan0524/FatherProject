package zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 测试ZooKeeper原生API实现的分布式锁
 * 启动10个线程，同时向zookeeper注册节点
 */
public class DistributedLockTest {

    //static Integer ALL = 10;

    public static void main(String[] args) throws IOException, InterruptedException {
        for (int j = 0; j < 10; j++) {
            CountDownLatch countDownLatch = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                new Thread(() -> {
                    try {
                        countDownLatch.await();
                        // 10线程并发执行以下代码
                        DistributedLockApp distributedLock = new DistributedLockApp();
                        // 获得锁
                        distributedLock.lock();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, "Thread-" + i).start();
                countDownLatch.countDown();
            }
            System.in.read();
        }
    }
}
