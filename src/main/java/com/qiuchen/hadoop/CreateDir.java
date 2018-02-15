package com.qiuchen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Create Dir on HDFS.
 */
public class CreateDir {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/new_dir";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path(uri));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
