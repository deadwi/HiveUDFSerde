package example.hive.serde.ux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import example.hive.serde.FsLineReader;

public class UXLogReader
{
	private static final String TARGET_INVAILD_EVENT = "INVAILD_EVENT";
	private FsLineReader reader;
	private ArrayList<UXClickEvent> itemlist = new ArrayList<UXClickEvent>();
	private int itemIndex;
	private long stopPos;
	
	public UXLogReader(FsLineReader reader, long stopPos)
	{
		this.reader = reader;
		this.stopPos = stopPos;
		clearStatus();
	}
	
	private void clearStatus()
	{
		itemlist.clear();
		itemIndex = 0;
	}
	
	public boolean IsEndItem()
	{
		return itemIndex>=itemlist.size();
	}

	public UXClickEvent next() throws IOException
	{
		if(IsEndItem()==false || findNextItem()==true)
			return itemlist.get(itemIndex++);
		return null;
	}
	
	private boolean findNextItem() throws IOException
	{
		clearStatus();

		String rawLine;
		while(true)
		{
			rawLine = getRawLog();
			if(rawLine == null)
				break;
			if(parseUXLog(rawLine)==true)
				return true;
		}
		return false;
	}
	
	private String getRawLog() throws IOException
	{
		String line = null;
		while(stopPos>reader.getPos())
		{
			line = reader.readLine();
			if(line==null)
				break;
      line = line.trim();
      if(!line.isEmpty())
        break;
		}
		return line;
	}
	
	private boolean parseUXLog(String line)
	{
		// timestamp,x,y,target;timestamp,x,y,target;...
    StringTokenizer stk = new StringTokenizer(line, ";", false);
    while (stk.hasMoreTokens())
    {
      String[] row = stk.nextToken().split(",",-1);
      UXClickEvent item = new UXClickEvent();
      if (row.length == 4) {
        item.timestamp = Long.parseLong(row[0]);
        item.posX = Integer.parseInt(row[1]);
        item.posY = Integer.parseInt(row[2]);
        item.target = row[3].trim();
      }
      else {
        item.timestamp = 0;
        item.posX = 0;
        item.posY = 0;
        item.target = TARGET_INVAILD_EVENT;
      }
      itemlist.add(item);
    }
		return !itemlist.isEmpty();
	}
}
