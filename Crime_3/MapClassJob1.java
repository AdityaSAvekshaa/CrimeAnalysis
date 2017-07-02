package com.hadoop.basic;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MapClassJob1 extends Mapper<LongWritable,Text,Text,IntWritable>
{
	IntWritable countVal =  new IntWritable(1);
	String code = "noVal";
public void setup(Context context){
	
	
}
	public void map(LongWritable key, Text value,Context context){
		try{
			String[] split = value.toString().split(",");
			 String desiredString = split[5];
			 if (desiredString.equals("THEFT")){
				 code =split[11];
			context.write(new Text(code), countVal);			
			 }
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
