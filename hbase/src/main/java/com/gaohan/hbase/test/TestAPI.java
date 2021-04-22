package com.gaohan.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 获取Configuration对象
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * DML:
 * 5.插入数据
 * 6.查数据(get)
 * 7.查数据(scan)
 * 8.删除数据
 */
public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            // 获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "192.168.44.12");
            // 创建连接对象
            connection = ConnectionFactory.createConnection(configuration);
            // 创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭资源
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    // 1.判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

        return admin.tableExists(TableName.valueOf(tableName));
    }

    // 2.创建表
    public static void createTable(String tableName, String... columnFamily) throws IOException {

        // 判断是否传入列族信息
        if (columnFamily.length == 0) {
            System.out.println("请传入列族信息");
            return;
        }

        if (isTableExist(tableName)) {
            System.out.println("表已存在");
        }else {
            // 创建表描述器
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            // 创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功");
        }
    }

    // 3.创建命名空间
    public static void createNameSpace(String nameSpace) {
        // 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        // 创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
            System.out.println("创建命名空间" + nameSpace + "成功");
        } catch (NamespaceExistException ns) {
            System.out.println(ns + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 4.删除表
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println("表不存在，无从删除");
        } else {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "已删除");
        }
    }

    // 5.插入数据
    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 给Put对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        // put插入数据
        table.put(put);
        // 关闭表连接
        table.close();

        System.out.println("表" + tableName + "插入数据成功");
    }

    // 6.获取具体数据 get
    public static void getData(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 如果获取某一行指定“列族: 列”的数据
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        // 获取数据
        Result result = table.get(get);
        // 解析result
        for (Cell cell : result.rawCells()) {
            // 打印数据
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)) + '\n' +
                    "列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)) + '\n' +
                    "值：" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        // 关闭表连接
        table.close();
    }

    // 7.获取全表数据 scan
    public static void scanTable(String tableName) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 构建Scan对象
        Scan scan = new Scan();
        // 扫描表
        ResultScanner resultScanner = table.getScanner(scan);
        // 解析resultScanner
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                // 打印数据
                System.out.println("键：" + Bytes.toString(CellUtil.cloneRow(cell)) + '\n' +
                        "列族：" + Bytes.toString(CellUtil.cloneFamily(cell)) + '\n' +
                        "列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)) + '\n' +
                        "值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    public static void main(String[] args) throws IOException {

        // 测试表是否存在
        System.out.println(isTableExist("stu"));

        // 创建表测试
//        createTable("createdTable", "info1", "info2");

        // 删除表测试
  //      dropTable("createdTable");

        // 创建命名空间测试
    //    createNameSpace("newNS");

        // 插入数据测试
   //     putData("stu", "1002", "info", "name", "gaohan1");

        // get查数据测试
    //    getData("stu", "1002", "info", "age");

        // scan扫描全表测试
        scanTable("stu");

        // 关闭资源
        close();

    }

}
