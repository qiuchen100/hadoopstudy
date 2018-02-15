package com.qiuchen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DeleteFile {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9000/input/new_dir";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        boolean isDeleted = fs.delete(new Path(uri));
        if (isDeleted) {
            System.out.println("Delete!");
        }
    }
}
