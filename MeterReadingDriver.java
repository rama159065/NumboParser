package com.demo.reading;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class MeterReadingDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MeterReadingDriver(), args);
		System.exit(exitCode);
	}
 
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
 
            Configuration conf = new Configuration();
 
            conf.set("START_TAG_KEY", "<MeterReadings>");
            conf.set("END_TAG_KEY", "</MeterReadings>");
 
            Job job = new Job(conf, "MeterReadings XML Processing ");
            job.setJarByClass(MeterReadingDriver.class);
            job.setMapperClass(MeterReadingMapper.class);
            //job.setReducerClass(MeterReadingReducer.class);
 
            //job.setNumReduceTasks(0);
 
            job.setInputFormatClass(XmlInputFormat.class);
            job.setOutputValueClass(TextOutputFormat.class);
 
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
 
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
 
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
            MultipleOutputs.addNamedOutput(job, "ConsumptionData", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "MaxDemandData",TextOutputFormat.class,Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "CumulativeDemandData",TextOutputFormat.class,Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "CoincidentDemandData",TextOutputFormat.class,Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "PresentDemandData",TextOutputFormat.class,Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "IntervalData",TextOutputFormat.class,Text.class, Text.class);
 
            int returnValue = job.waitForCompletion(true) ? 0:1;
    		System.out.println("job.isSuccessful " + job.isSuccessful());
    		return returnValue;
 
        } 
        
 
    }
 

 