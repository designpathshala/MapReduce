package com.designpathshala.hadoop.hive.jdbc;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://127.0.0.1:10000/dp", "hue", "");
		Statement stmt = con.createStatement();
		String tableName = "stocks";

		// describe table
		String sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// // load data into table
		// // NOTE: filepath has to be local to the hive server
		// // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per
		// line
		// String filepath = "/tmp/a.txt";
		// sql = "load data local inpath '" + filepath + "' into table " +
		// tableName;
		// System.out.println("Running: " + sql);
		// res = stmt.executeQuery(sql);

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t"
					+ res.getString(2) + "\t" + res.getString(3) + "\t"
					+ res.getString(4) + "\t" + res.getString(5) + "\t"
					+ res.getString(6));
		}

		// count number of records - creates map reduce code
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}