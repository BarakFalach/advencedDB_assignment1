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
        String insertStatementPrefix = "INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES (";
        String insertStatement = "";
        PreparedStatement preparedStatement = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                String[] movie = line.split(splitBy);
                System.out.println("Movie [Name=" + movie[0] + ", Year=" + movie[1]+ ']');
                insertStatement = insertStatementPrefix + "'" + movie[0] + "', '" + movie[1] + "');";
                preparedStatement = dbConnection.prepareStatement(insertStatement);
                preparedStatement.executeQuery();
            }
            assert preparedStatement != null;
            br.close();
            preparedStatement.close();
        }
        catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void calculateSimilarity() {

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


}
