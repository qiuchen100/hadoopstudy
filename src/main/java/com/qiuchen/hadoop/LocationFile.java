package com.qiuchen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LocationFile {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/words";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus fileStatus = fs.getFileStatus(new Path(uri));
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("Block Location: " + blockLocation.getHosts()[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
