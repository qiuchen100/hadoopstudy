package com.qiuchen.ad;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * CCreated by qiuchen on 2018/03/16.
 * 实现Writable接口，序列化
 */
public class AreaCntWritable implements Writable {
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
        out.writeInt(pv);
        out.writeInt(click);
    }

    public void readFields(DataInput in) throws IOException {
        pv = in.readInt();
        click = in.readInt();
    }
}
