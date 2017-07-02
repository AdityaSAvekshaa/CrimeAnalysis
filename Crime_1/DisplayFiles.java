package com.hadoop.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DisplayFiles {
	public void setConfig() throws IOException, ClassNotFoundException, InterruptedException{
	Configuration conf = new Configuration();
	conf.addResource(new Path("/usr/local/hadoop-2.6.0/etc/hadoop/core-site.xml"));
	conf.addResource(new Path("/usr/local/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"));
	//Path outputPath = new Path("/user/acadgild/crime_data");// ARGUMENT FOR OUTPUT_LOCATION
	//FileSystem fs = FileSystem.get(conf);
	//OutputStream os = fs.create(outputPath);
	//InputStream is = new BufferedInputStream(new FileInputStream(pathName));//Data set is getting copied into input stream through buffer mechanism.
	//IOUtils.copyBytes(is, os, conf); // Copying the dataset from input stream to output stream
	System.out.println("all done");
	
	//Create Job
	Job job = new Job(conf, "MapRed1");
	job.setJarByClass(DisplayFiles.class);
	
	// File Input and Output paths
	FileInputFormat.setInputPaths(job, "/user/acadgild/hadoop/crime_file.csv");
	FileOutputFormat.setOutputPath(job, new Path( "/user/acadgild/hadoop/Code32Count"));
	
	//Set Mapper class and Output format for key-value pair.
	job.setMapperClass(MapClassJob1.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	// define combiner class
	job.setCombinerClass(ReduceClassJob1.class);
	
	//Set Reducer class and Input/Output format for key-value pair.
	job.setReducerClass(ReduceClassJob1.class);
	
	//Number of Reducer tasks.
	//job.setNumReduceTasks(1);
	
	//Input and Output format for data
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
		        int returnValue = job.waitForCompletion(true) ? 0:1;
		        if(job.isSuccessful()) {
		            System.out.println("Job was successful");
		        } else if(!job.isSuccessful()) {
		            System.out.println("Job was not successful");          
		        }
	}
}

