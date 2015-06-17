package com.carewise.hbase.client;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by bdavay on 6/15/15.
 */
public class HBaseClient {
    public static void main(String[] arg) throws IOException {

        String zookeeperHost = "127.0.0.1";
        Configuration hBaseConfig =  HBaseConfiguration.create();
        hBaseConfig.set("hbase.zookeeper.quorum",zookeeperHost);
        hBaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");
        hBaseConfig.set("hbase.master", "127.0.0.1:60000");
        hBaseConfig.setInt("hbase.rpc.timeout",1200000);
        hBaseConfig.set("hbase.client.retries.number", Integer.toString(1));
        hBaseConfig.set("zookeeper.session.timeout", Integer.toString(60000));
        hBaseConfig.set("zookeeper.recovery.retry", Integer.toString(1));
        HBaseAdmin.checkHBaseAvailable(hBaseConfig);

        System.out.println("Successfully configured the hbase conf");

        HTable testTable = new HTable(hBaseConfig, "iemployee");


        for (int i = 0; i < 100; i++) {
            byte[] family = Bytes.toBytes("insurance");
            byte[] qual = Bytes.toBytes("dental");

            Scan scan = new Scan();
            scan.setMaxVersions(1);
            //scan.addFamily(family);
            scan.addColumn(family,qual);
            ResultScanner rs = testTable.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                System.out.println("Found row : " + r);
            }
        }


        Get get = new Get(Bytes.toBytes("1"));
        get.addFamily(Bytes.toBytes("insurance"));
       // get.setMaxVersions(3);
        Result result = testTable.get(get);
        KeyValue kv= result.getColumnLatest(Bytes.toBytes("insurance"), Bytes.toBytes("health"));

        System.out.println(kv);
        System.out.println("Found row : " + result);

        testTable.close();
    }

}
