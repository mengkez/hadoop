package lab.cards.mappers;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class RecordMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	
	public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException{
		context.write(new Text("count"), new IntWritable(1));
	}
}
