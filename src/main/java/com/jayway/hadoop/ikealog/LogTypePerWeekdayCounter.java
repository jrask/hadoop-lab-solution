package com.jayway.hadoop.ikealog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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


/**
 * TASK: Create a program that takes the output from {@link LogTypePerDateCounter} and calculates
 * logtypes (ERROR,WARN) per weekday (See expected output below) 
 * 
 * 
 * ==> For expected input, checkout {@link LogTypePerDateCounter}
 * 
 * <== Expected output <==
  	Fri ERROR	203
	Fri WARN	44333
	Mon ERROR	72
	Mon WARN	16772
	Sat ERROR	190
	Sat WARN	52872
	Sun ERROR	184
	Sun WARN	1144
	Thu ERROR	29
	Thu WARN	29
 *
 */
public class LogTypePerWeekdayCounter {

	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job();
		job.setJarByClass(RowLengthCounter.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LogtypePerDayCounterMapper.class);
		job.setReducerClass(LogtypePerDayCounterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	static class LogtypePerDayCounterMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {


		SimpleDateFormat weekdayFormat = new SimpleDateFormat("E");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		
		protected void map(LongWritable key, Text row,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			Calendar cal = Calendar.getInstance();
			String[] dateLogtypeAndCount = row.toString().split("\t");
			Date date = null;
			try {
				date = format.parse(dateLogtypeAndCount[0]);
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}
			cal.setTime(date);
			String logType = dateLogtypeAndCount[1];
			int count = Integer.parseInt(dateLogtypeAndCount[2]);
			
			context.write(new Text(weekdayFormat.format(date) + " " + logType), new IntWritable(count));
		};
	}
	
	
	static class LogtypePerDayCounterReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		protected void reduce(Text weekday, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int maxValue = 0;

			for (IntWritable count : arg1) {
				maxValue += count.get();
			}

			context.write(weekday, new IntWritable(maxValue));
		};

	}
}
