package in.masr.reonlyeradb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReonlyTeraDB {
	private Connection con;
	private String host;
	private String user;
	private String passwd;
	private int fetchSize = 100;

	/**
	 * Look for <b>com.teradata.jdbc.TeraDriver</b> in classpath and init
	 * variables to host, user, passwd.
	 * 
	 * @param host
	 * @param user
	 * @param passwd
	 * @throws TeraInitException
	 */
	public ReonlyTeraDB(String host, String user, String passwd)
			throws TeraInitException {
		this.host = host;
		this.user = user;
		this.passwd = passwd;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
		} catch (ClassNotFoundException e) {
			System.err
					.println("Can not find teradata jdbc jar file. Please include it in classpath!");
			e.printStackTrace();
			throw new TeraInitException();
		}
	}

	/**
	 * Close teradata connection
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		if (con != null && !con.isClosed())
			con.close();
	}

	/**
	 * Set fetch size. It is useful if you want to query a large result set. It
	 * will retrieve the result by fech size. Please do before method
	 * getResultSetBySQLFile and getResultSet.
	 * 
	 * @param size
	 */
	public void setFetchSize(int size) {
		this.fetchSize = size;
	}

	/**
	 * Try to connect to teradata, establish session and logon. Do it before any
	 * other method of ReonlyTeraDB. Usually, you should connect immediately
	 * after ReonlyTeraDB is created.
	 * 
	 * @throws SQLException
	 */
	public void connect() throws SQLException {
		con = DriverManager.getConnection("jdbc:teradata://" + host, user,
				passwd);
		System.out.println("Teradata " + host + " is connected.");
	}

	/**
	 * You can use a querytext that contains <b>"?"</b> in it. We will replace
	 * <b>"?"</b> by params in sequence one by one. It is very useful when you
	 * want to build dynamic SQL.<br/>
	 * <b>Note</b> : If your SQL is like <b>select * from A where str =
	 * 'haha?'</b> and in fact you do not want the replacement happen, this
	 * method will not work!!!
	 * 
	 * @param querytext
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getResultSetByTemplateSQL(String querytext, String[] params)
			throws SQLException {
		boolean canbreak = false;
		int i = 0;
		while (!canbreak) {
			if (querytext.indexOf('?') != -1) {
				querytext = querytext.replaceFirst("\\?", params[i]);
				i++;
			} else {
				canbreak = true;
			}
		}

		return getResultSet(querytext);
	}

	/**
	 * Very simple method, select sql and return ResultSet.
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getResultSet(String sql) throws SQLException {
		Statement stmt = con.createStatement();
		stmt.setFetchSize(fetchSize);
		ResultSet set = stmt.executeQuery(sql);
		return set;
	}
}
