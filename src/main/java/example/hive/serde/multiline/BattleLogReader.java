package example.hive.serde.multiline;

import example.hive.serde.FsLineReader;
import example.hive.serde.Names;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jihun.jo on 2017-03-09.
 */
class BattleLogReader extends MultiLineLogReader
{
  private FsLineReader reader;
  private String schemaName;
  private Vector<Map<String, String>> table = null;
  private int iterRowIndex;

  public BattleLogReader(FsLineReader reader, String schemaName)
  {
    this.reader = reader;
    this.schemaName = schemaName;
    table = new Vector<Map<String, String>>(20);
    clearStatus();
  }

  private void clearStatus()
  {
    table.clear();
    iterRowIndex=0;
  }

  private Map<String, String> getRowData()
  {
    if(table.isEmpty()==true || iterRowIndex>=table.size())
      return null;
    return table.get(iterRowIndex);
  }

  @Override
  public boolean IsEndItemInTable()
  {
    return getRowData()==null;
  }

  @Override
  public Map<String, String> nextInTable() throws IOException
  {
    Map<String, String> row = getRowData();
    // 데이터가 없거나 다 읽은 경우, 데이터 읽기
    if(row == null)
    {
      if(parse() == false)
        return null; // 파싱한 결과가 없다면 종료

      row = getRowData();
      // 새로 읽은 게임 로그에서 해당 table의 데이터가 없는 경우, 빈 결과 리턴
      if(row == null)
        return new TreeMap<String, String>();
    }
    iterRowIndex++;
    row.put(Names.TABLE_NAME, schemaName);
    return row;
  }

  private Map<String, String> splitToken(String text)
  {
    Map<String, String> dataSet = new TreeMap<String, String>();
    StringTokenizer stk = new StringTokenizer(text, " ,", false);
    while (stk.hasMoreTokens())
    {
      String[] subToken = stk.nextToken().split("=");
      if (subToken.length == 2)
        dataSet.put(subToken[0].trim().toLowerCase(), subToken[1].trim());
      else if (subToken.length == 1)
        dataSet.put(subToken[0].trim().toLowerCase(), "");
    }
    return dataSet;
  }

  private boolean parse() throws IOException
  {
    clearStatus();

    boolean started = false;
    String gameID = null;
    String gameDatetime = null;

    String line;
    while ((line = reader.readLine()) != null)
    {
      Map<String, String> dataSet = splitToken(line);
      if (!started && isBattleStartLine(dataSet)) {
        started = true;
        gameID = dataSet.get("gameid");
        gameDatetime = dataSet.get("datetime");
        if(schemaName=="battle")
          table.add(dataSet);
      }
      else if (started) {
        if(isBattleStartLine(dataSet)) {
          reader.unreadLine(line);
          break;
        }
        else if (schemaName=="battle_user" && dataSet.get("user")!=null) {
          dataSet.put("gameid", gameID);
          dataSet.put("datetime", gameDatetime);
          table.add(dataSet);
        }
        else if (schemaName=="battle_kill" && dataSet.get("kill")!=null) {
          dataSet.put("gameid", gameID);
          dataSet.put("datetime", gameDatetime);
          table.add(dataSet);
        }
      }
    }
    return started;
  }

  private boolean isBattleStartLine(Map<String, String> dataSet)
  {
    return dataSet.get("gameid") != null;
  }
}
