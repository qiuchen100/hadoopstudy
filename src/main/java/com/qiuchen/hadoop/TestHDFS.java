package com.qiuchen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit Test
 */
public class TestHDFS {

    @Test
    public void read() {
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

    @Test
    public void write() {
        String uri = "hdfs://master:9000/input/newinput";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FSDataOutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            out = fs.create(new Path(uri));
            out.write("how are you?".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(out);
        }
    }
}
