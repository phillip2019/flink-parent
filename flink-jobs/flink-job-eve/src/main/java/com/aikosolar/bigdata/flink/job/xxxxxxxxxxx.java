package com.aikosolar.bigdata.flink.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

public class xxxxxxxxxxx {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ods:ods_f_eqp_tube_recipe_oee");
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("end_time"));
        scan.setCaching(5000);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        long start = System.currentTimeMillis();
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            Cell cell = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("end_time"));


            System.out.println(new String(cell.getValueArray()));
        }
        rs.close();
        t.close();
        conn.close();
        System.out.println(System.currentTimeMillis() - start);
    }
}
