package com.hadoop.basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClassJob1 extends Reducer<Text,IntWritable,Text, IntWritable>
{
	IntWritable result = new IntWritable();
public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
{
	if ((key.toString()).equals("32"))
	{
	int sum = 0;
	for (IntWritable val : values)
	{
		sum +=val.get();
	}
	result.set(sum);
	context.write(key, result);
	}
}
}