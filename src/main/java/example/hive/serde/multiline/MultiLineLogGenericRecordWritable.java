package example.hive.serde.multiline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class MultiLineLogGenericRecordWritable implements Writable
{
	Map<String, String> record;

	public Object getRecord()
	{
		return record;
	}

	public void setRecord(Object record)
	{
		this.record = (Map<String, String>)record;
	}

	public MultiLineLogGenericRecordWritable()
	{
		record  = new TreeMap<String, String>();
	}

	public MultiLineLogGenericRecordWritable(Object record)
	{
		this.record = (Map<String, String>)record;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		for( Map.Entry<String, String> elem : record.entrySet() )
			out.writeChars(elem.getKey()+"\t"+elem.getValue()+"\n");
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		while(true)
		{
			String line = in.readLine();
			if(line==null)
				break;
			String tokens[] = line.split("\t");
			if(tokens.length!=2)
				continue;
			record.put(tokens[0], tokens[1]);
		}
	}
}
