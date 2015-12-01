package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalisysLogsCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Integer counter = 0;
		
		for (IntWritable appears : values) {
			counter += appears.get();
			System.out.println("counter=" + counter);
		}
		
		context.write(key, new IntWritable(counter));
	}
}
