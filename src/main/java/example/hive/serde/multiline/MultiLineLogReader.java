package example.hive.serde.multiline;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jihun.jo on 2017-03-09.
 */
abstract class MultiLineLogReader
{
	abstract boolean IsEndItemInTable();
	abstract Map<String, String> nextInTable() throws IOException;
}
