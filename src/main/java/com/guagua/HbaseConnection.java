package com.guagua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author guagua
 * @date 2023/2/20 14:27
 * @describe
 */
public class HbaseConnection {

    private static Connection connection = null;

    private static final ReentrantLock lock = new ReentrantLock();

    private HbaseConnection() {
    }

    public static Connection getConnection() {
        if (connection == null) {
            synchronized (HbaseConnection.class) {
                if (connection == null) {
                    try {
//                        Configuration conf = new Configuration();
//                        connection = ConnectionFactory.createConnection(conf);
                        connection = ConnectionFactory.createConnection();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }

    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    public static Connection getConnection() {
//        try {
//            if (connection == null) {
//                lock.lock();
//                if (connection == null) {
//                    Configuration conf = new Configuration();
//                    conf.set("hbase.zookeeper.quorum", "hadoop-01");
//                    connection = ConnectionFactory.createConnection(conf);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            lock.unlock();
//        }
//        return connection;
//    }

    public static void main(String[] args) throws IOException {
        Connection conn = getConnection();
        System.out.println(conn);

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Connection connection1 = getConnection();
        System.out.println(connection1);

        close();
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
