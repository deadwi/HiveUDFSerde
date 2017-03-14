package example.hive.serde.ux;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UXLogGenericRecordWritable implements Writable
{
	private UXClickEvent record;

	public Object getRecord()
	{
		return record;
	}

	public void setRecord(Object record)
	{
		this.record = (UXClickEvent)record;
	}

	public UXLogGenericRecordWritable()
	{
	}

	public UXLogGenericRecordWritable(Object record)
	{
		this.record = (UXClickEvent)record;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(record.timestamp);
		out.writeInt(record.posX);
		out.writeInt(record.posY);
		out.writeChars((record.target==null ? "" : record.target)+"\n");
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		record = new UXClickEvent();
		record.timestamp = in.readLong();
		record.posX = in.readInt();
		record.posY = in.readInt();
		record.target = in.readLine();
	}
}
