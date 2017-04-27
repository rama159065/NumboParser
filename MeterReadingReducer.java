package com.demo.reading;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MeterReadingReducer  extends Reducer<Text,Text,NullWritable,Text>{
	
    private MultipleOutputs mos;
	private final String  NEXT_LINE = "\n";
	private final String SEPERATOR = ",";
	private final String[] consumCols = {"DEVICE_NAME", "NODE_NAME", "UOM", "READING_TYPE", "DIRECTION", "TOUBUCKET", "MEASUREMENTPERIOD", "MULTIPLIER", "READING_TIME", "EST_TIME", "READING_VALUE"};
	private final String[] maxDemandCols = {"DEVICE_NAME", "NODE_NAME", "UOM", "READING_TYPE", "DIRECTION", "TOUBUCKET", "MEASUREMENTPERIOD", "MULTIPLIER", "READING_TIME", "EST_TIME", "READING_VALUE"};
	private final String[] loadSumCols = {"DEVICE_NAME", "NODE_NAME", "UOM", "READING_TYPE", "DIRECTION", "TOUBUCKET", "MEASUREMENTPERIOD", "MULTIPLIER", "READING_TIME", "EST_TIME", "READING_VALUE"};
    
    protected void setup(Context context){
    	
    	mos = new MultipleOutputs(context);
    	
    }
    
    public void reduce(Text key, Iterable<Text> values,  Context context ) throws IOException, InterruptedException {
    	if(key.equals("ConsumptionSpec")){
			String header = createHeader(consumCols);
			writeData(values, header, "Consumptions", "ConsumptionData");
		}else if(key.equals("MaxDemandSpec")){
			String header = createHeader(maxDemandCols);
			writeData(values, header, "Maxdemand", "MaxDemandData");
		}else if(key.equals("Channel")){
			String header = createHeader(loadSumCols);
			writeData(values, header, "LoadProfileSummary", "LoadProfileSummaryData");
		}
    }

    //method to write data to hdfs using multioutput format for a specific content type
    private void writeData(Iterable<Text> data, String header, String namedOutput, String outputPath){
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(header);
			sb.append(NEXT_LINE);
			for (Text value : data){
				sb.append(value);
				sb.append(NEXT_LINE);
			}
			mos.write(namedOutput, NullWritable.get(),new Text(data.toString()), outputPath);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	//creating header specific to column names
	private String createHeader(String[] cols){
		StringBuilder header = new StringBuilder();
		for (String col : cols){
			header.append(SEPERATOR);
			header.append(col);
		}
		return header.toString().substring(1);
	}

	//Generic header
    private String createHeader(){
    	StringBuilder header = new StringBuilder();
    	return header.append("DEVICE_NAME").append(SEPERATOR)
    	.append("NODE_NAME").append(SEPERATOR)
    	.append("UOM").append(SEPERATOR)
    	.append("READING_TYPE").append(SEPERATOR)
    	.append("DIRECTION").append(SEPERATOR)
    	.append("TOUBUCKET").append(SEPERATOR)
    	.append("MEASUREMENTPERIOD").append(SEPERATOR)
    	.append("MULTIPLIER").append(SEPERATOR)
    	.append("READING_TIME").append(SEPERATOR)
    	.append("EST_TIME").append(SEPERATOR)
    	.append("READING_VALUE").append(SEPERATOR).toString();

    }

}
