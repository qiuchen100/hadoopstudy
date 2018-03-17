package com.qiuchen.ad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiuchen on 2018/03/16
 * 需求2：对前一天产生的广告数据进行统计，需要完成如下功能
 统计粒度：按天统计
 统计频率：每天统计前一天的数据
 统计指标：曝光量pv，点击量click，点击率click_ratio
 统计维度：地域area_id
 */
public class PvAreaCountByDay {

    /**
     * CCreated by qiuchen on 2018/03/16.
     * 实现Writable接口，序列化
     */
    static class AreaCntWritable implements Writable {
        public AreaCntWritable() {
        }

        public AreaCntWritable(int pv, int click) {
            this.pv = pv;
            this.click = click;
        }

        int pv;
        int click;

        public int getPv() {
            return pv;
        }

        public void setPv(int pv) {
            this.pv = pv;
        }

        public int getClick() {
            return click;
        }

        public void setClick(int click) {
            this.click = click;
        }

        public void write(DataOutput out) throws IOException {
            out.write(pv);
            out.write(click);
        }

        public void readFields(DataInput in) throws IOException {
            pv = in.readInt();
            click = in.readInt();
        }
    }

    static class PvAreaCountMapper extends Mapper<LongWritable, Text, Text, AreaCntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String area_id = fields[0];
            String date = fields[3];
            int pv_cnt = 0;
            int click_cnt = 0;
            if (fields[2].equals("1")) {
                pv_cnt += 1;
            } else {
                click_cnt += 1;
            }
            String keyStr = area_id + "-" + date; //组合键
            context.write(new Text(keyStr), new AreaCntWritable(pv_cnt, click_cnt));
        }
    }

    static class PvAreaCountReducer extends Reducer<Text, AreaCntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<AreaCntWritable> values, Context context) throws IOException, InterruptedException {
            int pv_cnt = 0;
            int click_cnt = 0;
            for (AreaCntWritable value : values) {
                pv_cnt += value.getPv();
                click_cnt += value.getClick();
            }
            double clickRatio = (double)click_cnt / pv_cnt * 100;
            //保留两位小数
            String clickRatioStr = String.format("%.2f", clickRatio).toString() + "%";

            String[] keyset = key.toString().split("-");
            String line = keyset[0] + "\t" + keyset[1] + "\t" + pv_cnt + "\t" + click_cnt + "\t" + clickRatioStr;
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputUrl = args[0];
        String outputUrl = args[1];
        Path inputPath = new Path(inputUrl);
        Path outputPath = new Path(outputUrl);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PvAreaCountByDay");
        job.setJarByClass(PvAreaCountByDay.class);
        job.setMapperClass(PvAreaCountMapper.class);
        job.setReducerClass(PvAreaCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AreaCntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}