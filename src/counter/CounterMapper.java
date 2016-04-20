package counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<Object,Text,Text,IntWritable>{

	enum Temparature{MISSING,INVALID};
	
	protected void map(Object key,Text value,Context context) throws IOException, InterruptedException
	{
		String str[] = value.toString().split("\t");
		String dept;
		int sal;
		if (str[0].trim().charAt(0) !='1') 
			context.getCounter(Temparature.INVALID).increment(1);
		else if(str[2].trim().isEmpty() || str[2] == null){
			context.getCounter(Temparature.MISSING).increment(1);
		}
		else
		{
			dept = str[0];
			sal = Integer.parseInt(str[2]);
			context.write(new Text(dept), new IntWritable(sal));	
		}
		
	}
}
