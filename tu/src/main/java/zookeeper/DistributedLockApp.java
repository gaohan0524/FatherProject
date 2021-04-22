package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 20210411-20210417 使用ZK原生API完成分布式锁
 */
public class DistributedLockApp implements Lock, Watcher {

    private static final String ZK_ADDRESS = "192.168.44.12:2181";
    private ZooKeeper zkClient = null;
    private final String ROOT_LOCK = "/zkLock";
    private String WAIT_LOCK;
    private String CURRENT_LOCK;

    private CountDownLatch countDownLatch;

    public DistributedLockApp() {
        try {
            zkClient = new ZooKeeper(ZK_ADDRESS, 5000, this);
            // 判断根节点是否存在
            Stat stat = zkClient.exists(ROOT_LOCK, false);
            if (stat == null) {
                //创建持久化节点
                zkClient.create(ROOT_LOCK, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if (this.tryLock()) {
            // 如果获取锁成功
            System.out.println(Thread.currentThread().getName() + "->" + CURRENT_LOCK + "-> 成功获取锁");
            return;
        }
        try {
            //没有获得锁，继续等待
            waitForLock(WAIT_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean waitForLock(String prev) throws Exception{

        // 监听当前节点的前一个节点
        Stat stat = zkClient.exists(prev, true);
        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "-> 等待获得锁" + ROOT_LOCK + "/"
                    + prev + "释放");
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
            System.out.println(Thread.currentThread().getName()+"-> 成功获得锁");
        }
        return true;
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        try {
            //创建一个临时有序的节点
            CURRENT_LOCK = zkClient.create(ROOT_LOCK + "/",
                    "0".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + "->" + CURRENT_LOCK + "尝试竞争锁");
            // 获取根节点下的所有子节点，这里的watcher设置为false，因为不需要监听
            List<String> children = zkClient.getChildren(ROOT_LOCK, false);
            // 定义一个有序的集合进行排序
            SortedSet<String> sortedSet = new TreeSet<>();
            for (String child : children) {
                sortedSet.add(ROOT_LOCK + "/" + child);
            }
            // 获取当前所有子节点中最小的节点
            String firstNode = sortedSet.first();
            // 获取一个比当前节点小的节点
            SortedSet<String> lessThanMe = sortedSet.headSet(CURRENT_LOCK);
            // 将当前节点与子节点中的最小节点进行比较，如果它们相等，则获得锁
            if (CURRENT_LOCK.equals(firstNode)) {
                return true;
            }
            if (!lessThanMe.isEmpty()) {
                WAIT_LOCK = lessThanMe.last();
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        System.out.println(Thread.currentThread().getName() + "-> 释放锁" + CURRENT_LOCK);
        try {
            zkClient.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zkClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }
}
