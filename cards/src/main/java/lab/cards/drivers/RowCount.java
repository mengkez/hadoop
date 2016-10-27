package lab.cards.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import lab.cards.mappers.RecordMapper;
import lab.cards.reducers.NoKeyRecordCountReducer;
public class RowCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RowCount(), args);
		System.exit(exitCode);
	}
	
	
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Row Count using built in mappers and reducers");
		job.setJarByClass(getClass());
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		
		//mapper
		job.setMapperClass(RecordMapper.class);
		job.setMapOutputKeyClass(Text.class);//type of the output key
		job.setMapOutputValueClass(IntWritable.class);//type of the output value
		
		//reducer
		job.setReducerClass(NoKeyRecordCountReducer.class);
		job.setOutputKeyClass(NullWritable.class);//type of the output key Text.class/NullWritable.class
		job.setOutputValueClass(IntWritable.class);//type of the output value
		
		
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		return job.waitForCompletion(true)? 0:1;
	}

}
