package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalisysLogsCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

		String[] logValues = line.toString().split(" ");
		String month = logValues[0];
		String day = logValues[1];
		String hour = logValues[2].split(":")[0];
		String process = logValues[4];
		
		context.write(new Text(month + day + hour + "," + process), new IntWritable(1));
		
	}
}
