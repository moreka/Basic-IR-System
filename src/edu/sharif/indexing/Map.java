package edu.sharif.indexing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String filename = ((FileSplit)reporter.getInputSplit()).getPath().getName();
        IntWritable docID = new IntWritable(Integer.parseInt(filename.substring(3)));

        String line = value.toString();
        String[] tokens = line.split("\\s+");

        Text word = new Text();
        for (String token : tokens) {
            String[] list = getDictString(token);
            for (String item : list) {
                word.set(item);
                output.collect(word, docID);
            }
        }
    }

    private String[] getDictString(String input) {
//        if (input.matches("[-+]?[0-9]*\\.?[0-9]+"))
//            return new String[] { input };
//
//        if (input.matches("^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$"))
//            return new String[] { input };
//
//        if (input.matches("^(http|https|ftp)://[a-zA-Z0-9\\-\\.]+\\.[a-zA-Z]{2,3}(:[a-zA-Z0-9]*)?/?([a-zA-Z0-9\\-\\._\\?,\'/\\\\+&amp;%\\$#=~])*[^\\.,\\)\\(\\s]$"))
//            return new String[] { input };
//
        if (input.matches("[a-zA-Z]+")) {
            Stemmer stemmer = new Stemmer();
            stemmer.add(input.toLowerCase());
            return new String[]{stemmer.toString()};
        }
//
//        String[] parts = input.split("^\\W");


        return new String[] { input.toLowerCase() };
    }
}
