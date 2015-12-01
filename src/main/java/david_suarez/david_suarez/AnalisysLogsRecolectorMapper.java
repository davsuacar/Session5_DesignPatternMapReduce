package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalisysLogsRecolectorMapper extends Mapper<LongWritable, Text, Text, ProcessTuple>{
	
	public void map(LongWritable timeAndProcess, Text value, Context context)
			throws IOException, InterruptedException {
		String[] entry =  value.toString().split(":");
		String[] dateProcess = entry[0].split(",");
		String date = dateProcess[0];
		String process = dateProcess[1];
		
		IntWritable counter = new IntWritable(new Integer(entry[1].trim()));
		System.out.println("Emitting" + new Text(date) + new ProcessTuple(new Text(process), counter));
		
		ProcessTuple ourWritable = new ProcessTuple(new Text(process), counter);
		System.out.println("Object Created" + ourWritable);
		context.write(new Text(date), ourWritable);
		
	}
}
