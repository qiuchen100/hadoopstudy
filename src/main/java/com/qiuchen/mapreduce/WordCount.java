package com.qiuchen.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 自己写的一个WordCount小Demo
 */
public class WordCount {

    /**
     * WordCount Mapper
     */
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tonkenizer = new StringTokenizer(value.toString());
            Text word = new Text();
            IntWritable count = new IntWritable(1);
            while (tonkenizer.hasMoreTokens()) {
                word.set(tonkenizer.nextToken());
                context.write(word, count);
            }
        }
    }

    /**
     * WordCount Reducer
     */
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable count = new IntWritable();
            int cnt = 0;
            while (values.iterator().hasNext()) {
                cnt += 1;
            }
            count.set(cnt);
            context.write(key, count);
        }
    }

    /**
     * Word Count主函数
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("请依次输入作业的输入和输出地址!");
            System.exit(2);
        }
        String inputUrl = args[0];
        String outputUrl = args[1];
        Path inputPath = new Path(inputUrl);
        Path outputPath = new Path(outputUrl);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns");
        FileSystem fs = FileSystem.get(conf);
        FileStatus status = fs.getFileStatus(inputPath);
        if (!status.isDirectory()) {
            System.err.println("输入文件目录不存在!");
            System.exit(2);
        }
        status = fs.getFileStatus(outputPath);
        if (status.isDirectory()) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Word Count");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setJarByClass(WordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
