package edu.sharif.indexing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntArrayWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
        ArrayList<Writable> list = new ArrayList<Writable>();

        while (values.hasNext()) {
            list.add(new IntWritable(values.next().get()));
        }
        output.collect(key, new IntArrayWritable(list.toArray(new IntWritable[list.size()])));
    }
}
