package designMode.singleton;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Package designMode.singleton
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/6/18 13:41
 * @Email elonyong@163.com
 */
public class TestSing {
    public static void main(String[] args) {
        Connection connection = JDBCUtilSingle.INSTANCE.getConnection();
        Statement statement;
        try {
             statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
