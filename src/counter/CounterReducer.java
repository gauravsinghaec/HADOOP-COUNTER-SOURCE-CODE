package counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int maxValue = Integer.MIN_VALUE;
		for(IntWritable val:values){
			int x=Integer.parseInt(val.toString());
			maxValue=Math.max(x, maxValue);	
		}
		context.write(key, new IntWritable(maxValue));
	}
	
}

