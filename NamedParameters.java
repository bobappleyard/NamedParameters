import java.util.Calendar;
import java.util.Vector;
import java.util.regex.*;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

public class NamedParameters {
	// The regex includes extraneous patterns so as to avoid improperly recognising params
	private static final Pattern param = Pattern.compile("'.*?'|@\\w+"); 

	private Vector<String> names;
	private PreparedStatement statement;
	Connection connection;
	
	/* Private methods */
	
	private String processStmt(String s) {
		/* 
			Ooh, source-to-source transformations! I love those!

			Remember these words:

				"This module is a source filter.  Source filters always break." -- C. Salzenberg

			Fortunately, SQL syntax is of the less-crazy variety, and the '@' character followed by
			alphanums isn't commonly taken (as far as I know). So this shouldn't break too often.
		*/
		
		// Shared data 
		Vector<Integer> starts, ends;
		String result;
		
		starts = new Vector<Integer> (); 
		ends = new Vector<Integer> (); 
		
		// First, pull out the necessary info
		{
			Matcher m = param.matcher(s);
		
			names.clear();
			
			while (m.find()) {
				if (m.group().charAt(0) == '@') {
					names.add(m.group().substring(1));
					starts.add(m.start());
					ends.add(m.end());
				}
			}
		}
		
		// Then re-build the statement, replacing named parameters with '?'
		{
			int count = names.size();
			
			if (count == 0) {
				result = s;
			} else {			
				result = "";
				for (int i = 0; i < count; ++i) { // probably a better way of doing it than this...
					result += s.substring(i == 0 ? 0 : ends.elementAt(i - 1), starts.elementAt(i))
							+ "?"
							+ (i == count - 1 ? s.substring(ends.elementAt(i), s.length()) : "");
				}
			}
		}
		
		return result;
	}
	
	private int paramPos(String name) throws SQLException {
		for (int i = 0; i < names.size(); ++i) {
			if (name.equals(names.elementAt(i))) return i + 1;
		}
		throw new SQLException("Unknown parameter: "  + name);
	}
	
	/* Public methods */
	
	public NamedParameters(Connection c) {
		names = new Vector<String>();
		connection = c;
	}

	public void printParams() {
		for (String p : names) {
			System.out.println(p);
		}
	}
	
	public void setStatement(String s) throws SQLException {
		statement = connection.prepareStatement(processStmt(s));
	}
	
	public PreparedStatement getStatement() {
		return statement;
	}
	
	public boolean execute() throws SQLException {
		return statement.execute();
	}
	
	public ResultSet executeQuery() throws SQLException {
		return statement.executeQuery();
	}
	
	public int executeUpdate() throws SQLException {
		return statement.executeUpdate();
	}
	
	/* All the named setters */

	public void setArray(String name, Array x) throws SQLException {
		statement.setArray(paramPos(name), x);
	}

	public void setAsciiStream(String name, InputStream x, int length) throws SQLException {
		statement.setAsciiStream(paramPos(name), x, length);
	}

	public void setBigDecimal(String name, BigDecimal x) throws SQLException {
		statement.setBigDecimal(paramPos(name), x);
	}

	public void setBinaryStream(String name, InputStream x, int length) throws SQLException {
		statement.setBinaryStream(paramPos(name), x, length);
	}

	public void setBlob(String name, Blob x) throws SQLException {
		statement.setBlob(paramPos(name), x);
	}

	public void setBoolean(String name, boolean x) throws SQLException {
		statement.setBoolean(paramPos(name), x);
	}

	public void setBytes(String name, byte[] x) throws SQLException {
		statement.setBytes(paramPos(name), x);
	}

	public void setByte(String name, byte x) throws SQLException {
		statement.setByte(paramPos(name), x);
	}

	public void setCharacterStream(String name, Reader reader, int length) throws SQLException {
		statement.setCharacterStream(paramPos(name), reader, length);
	}

	public void setClob(String name, Clob x) throws SQLException {
		statement.setClob(paramPos(name), x);
	}

	public void setDate(String name, Date x) throws SQLException {
		statement.setDate(paramPos(name), x);
	}

	public void setDate(String name, Date x, Calendar cal) throws SQLException {
		statement.setDate(paramPos(name), x, cal);
	}

	public void setDouble(String name, double x) throws SQLException {
		statement.setDouble(paramPos(name), x);
	}

	public void setFloat(String name, float x) throws SQLException {
		statement.setFloat(paramPos(name), x);
	}

	public void setInt(String name, int x) throws SQLException {
		statement.setInt(paramPos(name), x);
	}

	public void setLong(String name, long x) throws SQLException {
		statement.setLong(paramPos(name), x);
	}

	public void setNull(String name, int sqlType) throws SQLException {
		statement.setNull(paramPos(name), sqlType);
	}

	public void setNull(String name, int sqlType, String typeName) throws SQLException {
		statement.setNull(paramPos(name), sqlType, typeName);
	}

	public void setObject(String name, Object x) throws SQLException {
		statement.setObject(paramPos(name), x);
	}

	public void setObject(String name, Object x, int targetSqlType) throws SQLException {
		statement.setObject(paramPos(name), x, targetSqlType);
	}

	public void setObject(String name, Object x, int targetSqlType, int scale) throws SQLException {
		statement.setObject(paramPos(name), x, targetSqlType, scale);
	}

	public void setRef(String name, Ref x) throws SQLException {
		statement.setRef(paramPos(name), x);
	}

	public void setShort(String name, short x) throws SQLException {
		statement.setShort(paramPos(name), x);
	}

	public void setString(String name, String x) throws SQLException {
		statement.setString(paramPos(name), x);
	}

	public void setTimestamp(String name, Timestamp x) throws SQLException {
		statement.setTimestamp(paramPos(name), x);
	}

	public void setTimestamp(String name, Timestamp x, Calendar cal) throws SQLException {
		statement.setTimestamp(paramPos(name), x, cal);
	}

	public void setTime(String name, Time x) throws SQLException {
		statement.setTime(paramPos(name), x);
	}

	public void setTime(String name, Time x, Calendar cal) throws SQLException {
		statement.setTime(paramPos(name), x, cal);
	}

	@Deprecated
	public void setUnicodeStream(String name, InputStream x, int length) throws SQLException {
		statement.setUnicodeStream(paramPos(name), x, length);
	}

	public void setURL(String name, URL x) throws SQLException {
		statement.setURL(paramPos(name), x);
	}
	
}