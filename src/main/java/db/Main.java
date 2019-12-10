package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.net.ssl.SSLException;

public class Main {
	static private final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

	// update USER, PASS and DB URL according to credentials provided by the
	// website:
	// https://remotemysql.com/
	// in future move these hard coded strings into separated config file or even
	// better env variables
	static private final String DB = "oqAQDdZ1FM";
	static private final String DB_URL = "jdbc:mysql://remotemysql.com/" + DB + "?useSSL=false";
	static private final String USER = "oqAQDdZ1FM";
	static private final String PASS = "SPzPh1TLd8";

	public static void printTable(Statement stmt) throws SQLException 
	{
		String sql = "SELECT * FROM flights";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {			
			System.out.format("Number %5s Origin %20s destinations %20s Distance %5d Price %5d\n",
					rs.getInt("num"), rs.getString("origin"), rs.getString("destination"),
					rs.getInt("distance"), rs.getInt("price"));
		}
	}
	public static void printTable2(Statement stmt) throws SQLException 
	{
		String sql = "SELECT ? FROM flights";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {			
			System.out.format("Number %5s Origin %20s destinations %20s Distance %5d Price %5d\n",
					rs.getInt("num"), rs.getString("origin"), rs.getString("destination"),
					rs.getInt("distance"), rs.getInt("price"));
		}
	}

	public static void main(String[] args) throws SSLException {
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			printTable(stmt);			
			System.out.println("______________________________________________");

			String sql;
			ResultSet rs;
			int newPrice;

			System.out.println("a: ");
			sql = "UPDATE flights SET price=2019 WHERE num=387";
			stmt.executeUpdate(sql);
			printTable(stmt);			
			System.out.println("______________________________________________");
//////////////////////////////////////////////////////////////
			System.out.println("b: ");
			sql = "SELECT price FROM flights WHERE num=387";
			rs = stmt.executeQuery(sql);
			rs.next();
			System.out.println("New Price for flight Num 387 is: " + rs.getInt("price"));
			printTable(stmt);			
			System.out.println("______________________________________________");
//////////////////////////////////////////////////////////////
			System.out.println("c1: ");
			Statement stmt2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
			sql = "SELECT num,price FROM flights WHERE distance > 1000";
			rs = stmt2.executeQuery(sql);
			while (rs.next())
			{
				newPrice = rs.getInt("price") + 100;
				rs.updateInt("price", newPrice);
				rs.updateRow();
			}
			printTable(stmt);			
			System.out.println("______________________________________________");
//////////////////////////////////////////////////////////////		
			System.out.println("c2: ");
			sql = "SELECT num,price FROM flights WHERE price < 300";
			rs = stmt2.executeQuery(sql);
			while (rs.next())
			{
				newPrice = rs.getInt("price") - 25;
				if(newPrice < 0)
					newPrice = 1;
				rs.updateInt("price", newPrice);
				rs.updateRow();
			}
			printTable(stmt);			
			System.out.println("______________________________________________");
			
			stmt.close();
//////////////////////////////////////////////////////////////
			System.out.println("d1: ");
			sql = "UPDATE flights SET price = price + ? WHERE distance > ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1,100);
			pstmt.setInt(2,1000);
			pstmt.executeUpdate();
			printTable(pstmt);			
			System.out.println("______________________________________________");
//////////////////////////////////////////////////////////////
			System.out.println("d2: ");
			sql = "UPDATE flights SET price = price - ? WHERE price < ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1,25);
			pstmt.setInt(2,300);
			pstmt.executeUpdate();
			printTable(pstmt);			
			System.out.println("______________________________________________");
			
			pstmt.close();			
		}
		
		catch (SQLException se)
		{
			se.printStackTrace();
			System.out.println("SQLException: " + se.getMessage());
			System.out.println("SQLState: " + se.getSQLState());
			System.out.println("VendorError: " + se.getErrorCode());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally 
		{
			try
			{
				if (pstmt != null)
					pstmt.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			}
		}
	}
};
