package com.jayway.hadoop.ikealog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.jayway.hadoop.ikealog.LogTypePerWeekdayCounter.LogtypePerDayCounterMapper;
import com.jayway.hadoop.ikealog.LogTypePerWeekdayCounter.LogtypePerDayCounterReducer;
import com.jayway.hadoop.util.IkeaLogUtils;


/**
 * 
 * 

 *
 */
public class LogTypePerDateCounter {

	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job();
		job.setJarByClass(RowLengthCounter.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LogTypePerWeekdayCounterMapper.class);
		job.setReducerClass(LogTypePerWeekdayCounterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	static class LogTypePerWeekdayCounterMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {


		SimpleDateFormat format2 = new SimpleDateFormat("yyyyMMdd");

		protected void map(LongWritable key, Text row,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			MatchResult result = IkeaLogUtils.entry(key, row);
			if (result == null) {
				return;
			}

			Date date = null;
			try {
				date = IkeaLogUtils.format.parse(result.group(1));
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}

			String logType = result.group(3);
			context.write(new Text(format2.format(date) + "\t" + logType),
					new IntWritable(1));
		};
	}

	static class LogTypePerWeekdayCounterReducer extends
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
