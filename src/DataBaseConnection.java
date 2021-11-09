import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;


public class DataBaseConnection {
    private final String connectionString;
    private final String userName;
    private final String password;

    public DataBaseConnection(String connectionString, String userName, String password) {
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
    }

    public void fileToDataBase(String path) {
        Connection dbConnection = createConnection();
        if (dbConnection == null) return;
        String line;
        String splitBy = ",";
        String insertStatement = "INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES (?, ?)";
        PreparedStatement preparedStatement;
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            preparedStatement = dbConnection.prepareStatement(insertStatement);
            while ((line = br.readLine()) != null) {
                String[] movie = line.split(splitBy);
                preparedStatement.setString(1, movie[0]);
                preparedStatement.setString(2, movie[1]);
                preparedStatement.executeQuery();
            }
            br.close();
            preparedStatement.close();
        }
        catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void calculateSimilarity() {
        Connection dbConnection = createConnection();
        if (dbConnection == null) return;
        int maximalDistance = maximalDistance(dbConnection);


    }

    public void printSimilarItems(long mid) {

    }

    private Connection createConnection()  {
        try{
        Connection dbConnection;
        Class.forName("oracle.jdbc.driver.OracleDriver");
        dbConnection = DriverManager.getConnection(this.connectionString, this.userName, this.password);
        return dbConnection;
        }
        catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    private int maximalDistance(Connection dbConnection) {
        if (dbConnection == null) return 0;
        try {
            CallableStatement callableStatement = dbConnection.prepareCall("{call MaximalDistance()}");
            callableStatement.execute();
            callableStatement.registerOutParameter(1, Types.INTEGER);
            return callableStatement.getInt(1);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }


}
