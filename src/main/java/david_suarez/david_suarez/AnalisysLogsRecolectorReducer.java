package david_suarez.david_suarez;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalisysLogsRecolectorReducer extends Reducer<Text, ProcessTuple, Text, Text>{
	
	public void reduce(Text key, Iterable<ProcessTuple> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Entering Reducer" + key + values.toString());
		String value = "";
//		for (ProcessTuple processTuple : values) {
//			value += processTuple.getProcess() + ":" + processTuple.getAppearances() + ",";
//		}
		context.write(key, new Text(value));
	}
}
