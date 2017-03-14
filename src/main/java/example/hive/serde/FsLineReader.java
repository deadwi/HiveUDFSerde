package example.hive.serde;

import java.io.EOFException;
import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.FSDataInputStream;

public class FsLineReader
{
	protected FSDataInputStream in;
	protected Stack<String> unlines;
	protected byte[] buffer;
	final protected int BUFFER_SIZE = 1024 * 8; 
	
	public FsLineReader(FSDataInputStream in)
	{
		this.in = in;
		buffer = new byte[BUFFER_SIZE];
		unlines = new Stack<String>();
	}
	
	public void seek(long pos) throws IOException
	{
		in.seek(pos);
	}
	
	public void seekByNewline(long pos) throws IOException
	{
		if(pos!=0)
		{
			// skip incomplete line
			in.seek(pos-1);
			readLine();
		}
		else
			in.seek(pos);
	}
	
	public long getPos() throws IOException
	{
		return in.getPos();
	}
	
	public void close() throws IOException
	{
		in.close();
	}
	
	public String readLine() throws IOException
	{
		if(unlines.isEmpty()==false)
			return unlines.pop();
		
		int count=0;
		byte chr;
		while(true)
		{
			try
			{
				chr = in.readByte();
			}
			catch(EOFException e)
			{
				if(count==0)
					return null;
				break;
			}
			if(chr=='\n')
				break;
			if(count>=BUFFER_SIZE)
				throw new IOException("Over line buffer : "+new String(buffer,0,count));
			buffer[count++]=chr;
		}
		return new String(buffer,0,count);
	}
	
	public void unreadLine(String line)
	{
		unlines.push(line);
	}
}
