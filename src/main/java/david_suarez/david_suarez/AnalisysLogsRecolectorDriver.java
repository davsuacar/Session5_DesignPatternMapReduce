package david_suarez.david_suarez;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalisysLogsRecolectorDriver {
	public static void main(String[] args) throws Exception {
		
		//Take arguments from other jobs.
		if (args.length != 2) {
			System.out.printf("Usage: AnalisysLogsRecolector <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(AnalisysLogsRecolectorDriver.class);
		job.setJobName("AnalisysLogsRecolector");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AnalisysLogsRecolectorMapper.class);
		job.setReducerClass(AnalisysLogsRecolectorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProcessTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
		
	}
}
