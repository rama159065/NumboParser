package com.demo.reading;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 
public class XmlInputFormat extends TextInputFormat {
    public static final String START_TAG_KEY = "<MeterReadings>";
    public static final String END_TAG_KEY = "</MeterReadings>";
 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }
 
    public static class XmlRecordReader extends
            RecordReader<LongWritable, Text> {
        private byte[] startTag;
        private byte[] startTagWithAttr;
        private byte[] endTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();
 
        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac)
                throws IOException, InterruptedException {
        	System.out.println("XmlInputFormat : initilize");
            FileSplit fileSplit = (FileSplit) is;
            String START_TAG_KEY = "<MeterReadings>";
            String START_TAG_WITH_ATR = START_TAG_KEY.substring(0, START_TAG_KEY.length()-1);
            System.out.println("START_TAG_WITH_ATR : "+START_TAG_WITH_ATR);
            String END_TAG_KEY = "</MeterReadings>";
            startTag = START_TAG_KEY.getBytes("utf-8");
            startTagWithAttr = START_TAG_WITH_ATR.getBytes("utf-8");
            endTag = END_TAG_KEY.getBytes("utf-8");
 
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
 
            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
            System.out.println("XmlInputFormat : initilize completed ");
 
        }
 
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
        	System.out.println("XmlInputFormat : nextKeyValue ");
            if (fsin.getPos() < end) {
            	System.out.println("XmlInputFormat : nextKeyValue in if");
                if (readUntilMatch(startTag, false, true)) {
                	System.out.println("XmlInputFormat : Found start tag");
                    try {
                        //buffer.write(startTag);
                        if (readUntilMatch(endTag, true, false)) {
 
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            System.out.println("XmlInputFormat : nextKeyValue completed");
            return false;
        }
 
        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }
 
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
 
        }
 
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }
 
        @Override
        public void close() throws IOException {
            fsin.close();
        }
 
        private boolean readUntilMatch(byte[] match, boolean withinBlock, boolean isStartingTag)
                throws IOException {
        	System.out.println("XmlInputFormat : readUntilMatch");
            int i = 0;
            while (true) {
            	//System.out.println("XmlInputFormat : readUntilMatch in loop");
                int b = fsin.read();
 
                if (b == -1)
                    return false;
 
                if (withinBlock)
                    buffer.write(b);
 
                if (b == match[i]) {
                    i++;
                    System.out.println("increasing in i value "+i);
                    if (i >= match.length){
                    	if(isStartingTag)
                    	buffer.write(match);
                        return true;
                        }
                } else if(i==match.length-1 && isStartingTag){
                    //else if(i==match.length && isStartingTag && match[i] != b){
                    System.out.println("I and match lenght are equal "+i);
                	buffer.write(startTagWithAttr);
                	buffer.write(b);
                	return true;
                }else
                    i = 0;
 
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
            
            
        }
 
    }
 
}
 