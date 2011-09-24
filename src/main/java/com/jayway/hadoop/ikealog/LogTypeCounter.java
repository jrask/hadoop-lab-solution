package com.jayway.hadoop.ikealog;

import java.io.IOException;
import java.util.regex.MatchResult;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jayway.hadoop.gutenberg.RowLengthCounter;
import com.jayway.hadoop.util.IkeaLogUtils;


/**
 * <p>TASK: Write a program that outputs Logtype and count</p>
 * 
 * bin/hadoop jar ${path.to}/hadoop-lab-1.0-SNAPSHOT.jar com.jayway.hadoop.ikealog.LogTypeCounter /user/hduser/ikealogs /user/hduser/{you}/ikealogs-out{nr}
 * 
 * 
 * Expected result:
 * ERROR	678
 * WARN	115150
 * 
 * @author johanrask
 *
 */
public class LogTypeCounter {

	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job();
		job.setJarByClass(RowLengthCounter.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LogTypeCounterMapper.class);
		job.setReducerClass(LogTypeCounterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * IN => key:rownum, value:rowtext OUT => key:TYPE(ERROR,WARN), value:1
	 * 
	 *
	 */
	static class LogTypeCounterMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		protected void map(LongWritable key, Text row,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			MatchResult result = IkeaLogUtils.entry(key, row);			
			if(result == null) {
				return;
			}	
			String logType = result.group(3);
			context.write(new Text(logType), new IntWritable(1));
		};

	}

	/**
	 * IN => key:LogType, value:iterable={1,1,1,1,1,...} => key:TYPE(ERROR,WARN), value:total
	 *
	 */
	static class LogTypeCounterReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		protected void reduce(Text logType, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int maxValue = 0;

			for (IntWritable count : arg1) {
				maxValue += count.get();
			}

			context.write(logType, new IntWritable(maxValue));
		};

	}
}
