package sql_connect;
import java.sql.*;


public class test_sql_connection {
    public static void main(String[] args) throws Exception{
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/test_kafka","root","kishan");
            Statement stmt=con.createStatement();
            Statement stmt2=con.createStatement();
            Statement stmt3=con.createStatement();
            ResultSet rs, rs2,rs3;


            rs2 = stmt2.executeQuery("select max(id) from test_kafka");
            String sql = "delete from store_id";
            String sql2 = "select * from test_kafka where id >= "+ rs2.getInt(1);
            //rs=stmt.executeQuery("select * from test_kafka");
            rs=stmt.executeQuery(sql2);
           // rs3 = stmt3.executeUpdate("delete from store_id");
            while(rs.next())
                System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3) );

            stmt3.executeUpdate(sql);
            System.out.println("deleted succesfully");

            while (rs2.next())
                stmt2.executeUpdate("insert into store_id values (" + rs2.getInt(1) + ")");
              //  System.out.println("insert into store_id values (" + rs2.getInt(1) + ")");

            con.close();


        }catch(Exception e){ System.out.println(e);}
    }
}
