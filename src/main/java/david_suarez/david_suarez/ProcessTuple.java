package david_suarez.david_suarez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProcessTuple implements WritableComparable<ProcessTuple> {
	
	private Text process;
	private IntWritable appearances;
	
	public ProcessTuple(Text process, IntWritable appearances) {
		super();
		this.process = process;
		this.appearances = appearances;
	}

	public Text getProcess() {
		return process;
	}

	public void setProcess(Text process) {
		this.process = process;
	}

	public IntWritable getAppearances() {
		return appearances;
	}

	public void setAppearances(IntWritable appearances) {
		this.appearances = appearances;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((appearances == null) ? 0 : appearances.hashCode());
		result = prime * result + ((process == null) ? 0 : process.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProcessTuple other = (ProcessTuple) obj;
		if (appearances == null) {
			if (other.appearances != null)
				return false;
		} else if (!appearances.equals(other.appearances))
			return false;
		if (process == null) {
			if (other.process != null)
				return false;
		} else if (!process.equals(other.process))
			return false;
		return true;
	}

	public void write(DataOutput out) throws IOException {
		process.write(out);
		appearances.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		process.readFields(in);
		appearances.readFields(in);
		
	}

	public int compareTo(ProcessTuple o) {
		int cmp = process.compareTo(o.process);
		if (cmp != 0) {
			return cmp;
		} else
			return appearances.compareTo(o.getAppearances());
	}
}
