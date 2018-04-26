import java.sql.*;

/**
 * Created by dingbingbing on 4/26/18.
 */
public class PhoenixTest {
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws SQLException {
        //UPSERT  INTO eventlog ("EVENTID", "EVENTTIME", "0"."EVENTTYPE", "0"."NAME", "0"."BIRTH", "0"."AGE") VALUES (?, ?, ?, ?, ?, ?)
        PreparedStatement stmt = null;
        ResultSet rs = null;

        Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
        String sql = "UPSERT  INTO eventlog (\"EVENTID\", \"EVENTTIME\", \"0\".\"EVENTTYPE\", \"0\".\"NAME\", \"0\".\"BIRTH\", \"0\".\"AGE\") VALUES (?, ?, ?, ?, ?, ?)";
        stmt = con.prepareStatement(sql);

        stmt.setLong(1, 1);
        stmt.setTimestamp(2, new Timestamp(1));
        stmt.setString(3, "t");
        stmt.setString(4, "Lk");
        stmt.setNull(5, 93);
        stmt.setNull(6, 4);


        stmt.execute();
        con.commit();

        stmt.close();
        con.close();
    }
}
