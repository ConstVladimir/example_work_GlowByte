package kvv.laptop;

import org.h2.tools.Console;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class main {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 2: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();

            String sql;
            //Создание 1 т
            /*sql =  "CREATE TABLE   TABLE_LIST (" +
                    " TABLE_NAME VARCHAR(32) not NULL," +
                    " PK VARCHAR(256)," +
                    " PRIMARY KEY ( TABLE_NAME ) )";
            crtTbl (stmt,sql);
            //Создание 2 т
            sql =  "CREATE TABLE   TABLE_COLS (" +
                    " TABLE_NAME VARCHAR(32) not NULL," +
                    " COLUMN_NAME VARCHAR(32) not NULL," +
                    " COLUMN_TYPE VARCHAR(32)," +
                    " PRIMARY KEY ( TABLE_NAME, COLUMN_NAME ) )";
            crtTbl (stmt,sql);

            //Занесекние данных
            sql = "INSERT INTO TABLE_LIST " +
                    "VALUES ('users','ID')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_LIST " +
                    "VALUES ('accounts', 'account, account_id')";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('users', 'first_name', 'VARCHAR(32)')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('users', 'second_name', 'VARCHAR(32)')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('users', 'id', 'INT')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('accounts', 'register_date', 'TIMESTAMP')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('accounts', 'CARD_NUMBER', 'INT')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('accounts', 'ACCOUNT', 'VARCHAR(32)')";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO TABLE_COLS " +
                    "VALUES ('accounts', 'ACCOUNT_ID', 'INT')";
            stmt.executeUpdate(sql);*/

            Map<String,String> all = new HashMap<String,String>();
            Map<String,Boolean> need = new HashMap<String,Boolean>();

            sql = "SELECT LOWER(TABLE_NAME), LOWER(COLUMN_NAME), LOWER(COLUMN_TYPE)  FROM TABLE_COLS";
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()){
                String t_name = rs.getString(1);
                String c_name = rs.getString(2);
                String c_type = rs.getString(3);
                all.put(t_name + ", " + c_name, c_type);
            }
            rs.close();

            sql = "SELECT LOWER(TABLE_NAME), LOWER(PK) FROM TABLE_LIST";
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                String t_name = rs.getString(1);
                ArrayList <String> pk = new ArrayList<>(Arrays.asList(rs.getString(2).split(", ")));
                for (String i : pk){
                    need.put(t_name + ", " + i, true);
                }
            }
            rs.close();
            stmt.close();
            conn.close();

            try(FileWriter writer = new FileWriter("text.txt", false))
            {
                for (String k : need.keySet())
                {
                    System.out.println(k + "  " + all.get(k));
                    writer.write(k + "  " + all.get(k) + "\n");
                }
                writer.flush();
            }
            catch(IOException ex){
                System.out.println(ex.getMessage());
            }




        } catch(SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            } // nothing we can do
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            } //end finally try
        } //end try
        System.out.println("Finish");
    }
    static ArrayList <String> getTblNms (Statement stmt) throws SQLException {
        ArrayList <String> rslt = new ArrayList<String>();
        String sql = "SHOW TABLES";
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
            //System.out.println( rs.getString("TABLE_NAME"));
            rslt.add(rs.getString("TABLE_NAME"));
        }
        rs.close();
        return rslt;
    }
    static boolean cheking (Statement stmt) throws SQLException {
        String sql = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLE_LIST';";
        boolean rs = stmt.execute(sql);
        /*while(rs.next()){

        }
        rs.close();*/
        return  rs;
    }
    static int crtTbl (Statement stmt, String sql) throws SQLException {
        System.out.println("Creating table in given database...");
        return stmt.executeUpdate(sql);
    }
}
