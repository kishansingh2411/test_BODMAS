package sql_connect;

import java.sql.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import sql_connect.*;

public class test_batch_stmt {
    public static void main(String[] args) throws Exception{
       // ouptut1 obj = new ouptut1();
        test_outout obj = new test_outout();
        ArrayList<String> s = obj.sqlstmt();

       for(int i =0; i < s.size();i++) {System.out.println((s.get(i)).toString());
       }

       /* try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con= DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/test_kafka","root","kishan");
            Statement stmt1=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt2=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt3=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt4=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs1,rs2,rs3;

            String sql1 = "select id from store_id";
           rs1 = stmt1.executeQuery(sql1);
           rs1.next();
           System.out.println(rs1.getInt(1));
           Integer id = rs1.getInt(1);

            String sql2 = "select * from test_kafka where id > "+ id;
            rs2 = stmt2.executeQuery(sql2);
            while(rs2.next())
            System.out.println(rs2.getInt(1) + "   "+rs2.getString(2) + "  "+rs2.getInt(3));

            String sql3 = "select max(id) from test_kafka";
            rs3 = stmt3.executeQuery(sql3);
            rs3.next();
            Integer max_id = rs3.getInt(1);

            String sql4 = "delete from store_id";
            String sql5 = "insert into store_id values (" + max_id + ")";
            stmt4.executeUpdate(sql4);
            stmt4.executeUpdate(sql5);
            con.close();


        }catch(Exception e){ System.out.println(e);}*/
    }
}

class ouptut1 {
    /*public ArrayList<String> sqlstmt (){
        String output="";
        ArrayList<String> arr = new ArrayList<String>();
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con= DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/test_kafka","root","kishan");
            Statement stmt1=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt2=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt3=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            Statement stmt4=con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs1,rs2,rs3;

            String sql1 = "select id from store_id";
            rs1 = stmt1.executeQuery(sql1);
            rs1.next();
            //System.out.println(rs1.getInt(1));
            Integer id = rs1.getInt(1);

            String sql2 = "select * from test_kafka where id > "+ id;
            rs2 = stmt2.executeQuery(sql2);
            while(rs2.next())
                arr.add(rs2.getInt(1) + "   "+rs2.getString(2) + "  "+rs2.getInt(3));
                //output = rs2.getInt(1) + "   "+rs2.getString(2) + "  "+rs2.getInt(3);
            //System.out.println(rs2.getInt(1) + "   "+rs2.getString(2) + "  "+rs2.getInt(3));

            String sql3 = "select max(id) from test_kafka";
            rs3 = stmt3.executeQuery(sql3);
            rs3.next();
            Integer max_id = rs3.getInt(1);

            String sql4 = "delete from store_id";
            String sql5 = "insert into store_id values (" + max_id + ")";
            stmt4.executeUpdate(sql4);
            stmt4.executeUpdate(sql5);
            con.close();


        }catch(Exception e){ System.out.println(e);}
        return arr;
    }*/
}