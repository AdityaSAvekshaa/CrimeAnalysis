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
			code = split[14];
			context.write(new Text(code), countVal);			
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
