package com.qiuchen.ad;

import org.apache.hadoop.conf.Configuration;
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

/**
 * 需求1：一批TB或者PB量级的历史广告数据，需要完成如下功能
 统计粒度：按天统计
 统计指标：计算曝光量（PV）
 按照曝光量升序排列和倒序排列
 */
public class PvSortByDay {
    static class PvCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            Text date = new Text();
            IntWritable pv_cnt = new IntWritable(1);
            if (fields[2].equals("1")) {
                date.set(fields[3]);
                context.write(date, pv_cnt);
            }
        }
    }

    static class PvCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable value : values) {
                cnt += value.get();
            }
            context.write(key, new IntWritable(cnt));
        }
    }

    static class PvSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            Text date = new Text(fields[0]);
            IntWritable pv_cnt = new IntWritable(Integer.parseInt(fields[1]));
            context.write(pv_cnt, date);
        }
    }

    static class PvSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    static class DscePvComparator extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputUrl = args[0];
        String outputUrl = args[1];
        String outputUrl2 = args[2];
        Path inputPath = new Path(inputUrl);
        Path outputPath = new Path(outputUrl);
        Path outputPath2 = new Path(outputUrl2);

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "PvSortByDay-1");
        job1.setJarByClass(PvSortByDay.class);
        job1.setMapperClass(PvCountMapper.class);
        job1.setReducerClass(PvCountReducer.class);
        job1.setCombinerClass(PvCountReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        boolean pvCountJob = job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "PvSortByDay-2");
        job2.setJarByClass(PvSortByDay.class);
        job2.setMapperClass(PvSortMapper.class);
        job2.setReducerClass(PvSortReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setSortComparatorClass(DscePvComparator.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        if (pvCountJob) {
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
