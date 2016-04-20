package counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterDriver {

	public static void main(String arg[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MR-Counter");
		
		job.setJarByClass(CounterDriver.class);
		
		FileInputFormat.addInputPath(job,new Path(arg[0]));
		FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
		job.setMapperClass(CounterMapper.class);
		job.setReducerClass(CounterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
