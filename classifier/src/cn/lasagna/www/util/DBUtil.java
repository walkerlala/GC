package cn.lasagna.www.util;

/**
 * Created by walkerlala on 16-10-23.
 */

import java.sql.*;

public class DBUtil {
    private Connection conn;
    private String driver;
    private String dbUrl;
    private String url;
    private String username;
    private String password;

    public DBUtil() {
        //default setting here
        this.driver = "com.mysql.jdbc.Driver";
        this.username = "root";
        this.password = "123456";
        this.dbUrl    =  "jdbc:mysql://localhost:3306";
    }

    public void connectDB(String databaseName) throws Exception {
        this.url = this.dbUrl.replaceAll("/+$", "") + "/" + databaseName + "?useUnicode=true&characterEncoding=UTF-8";
        getConnection();
    }

    // default username is root
    public void connectDB(String dbUrl, String user, String password, String databaseName) throws Exception {
        this.dbUrl = dbUrl;
        this.password = password;
        this.username = user;
        this.url = this.dbUrl.replaceAll("/+$", "") + "/" + databaseName + "?useUnicode=true&characterEncoding=UTF-8";
        getConnection();
    }

    public Connection getConnection() throws Exception {
        if(conn == null) {
            Class.forName(this.driver);
            conn = DriverManager.getConnection(this.url, this.username, this.password);
        }
        return conn;
    }

    /* Object...args is just a syntactic sugar for Object[] args. When passing multiple arguments,
    * the compiler automatically construct a array using all the arguments. For example, below are same:
    *      String.format("%s %s", "hello", "world!");
    *      String.format("%s %s", new Object[] { "hello", "world!"});
    */
    public boolean insert(String sql, Object... args) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            pstmt.setObject(i+1, args[i]);
        }
        int exeNUm;
        try{
            exeNUm = pstmt.executeUpdate();
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        // insert one row each time
        return (exeNUm == 1);
    }

    public ResultSet query(String sql, Object... args) throws Exception {
        PreparedStatement pstmt = getConnection().prepareStatement(sql);
        for(int i = 0; i < args.length; i++) {
            pstmt.setObject(i+1, args[i]);
        }
        return pstmt.executeQuery();
    }

    public synchronized void modify(String sql, Object... args) throws Exception {
        conn.setAutoCommit(false);

        PreparedStatement pstmt = conn.prepareStatement(sql);
        for(int i = 0; i< args.length; i++) {
            pstmt.setObject(i+1, args[i]);
        }

        try {
            pstmt.executeUpdate();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
        }

        conn.setAutoCommit(true);
        pstmt.close();
    }

    public void closeDBConn() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
