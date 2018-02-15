package com.qiuchen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;

/**
 * Read file on hdfs
 *
 */
public class FileSystemCat {
    public static void main(String[] args) {
        String uri = "hdfs://master:9000/input/words";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        InputStream in = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
