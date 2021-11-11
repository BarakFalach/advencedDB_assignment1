import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class DataBaseConnection {
    private final String connectionString;
    private final String userName;
    private final String password;
    private int maxDistance;

    public DataBaseConnection(String connectionString, String userName, String password) {
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
        this.maxDistance = 0;
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
        List<Long> movieIds = new ArrayList<>();
        Connection dbConnection = createConnection();
        float currentSimilarity = 0;
        if (dbConnection == null) return;
        calculateMaximalDistance(dbConnection);
        movieIds = getAllMovieIDs(dbConnection);
        // loop on loop
        int i = 0;
        int j = 1;
        currentSimilarity = getSingleSimilarity(dbConnection,movieIds.get(i), movieIds.get(j));
        writeSingleSimilarity(dbConnection,movieIds.get(i), movieIds.get(j),currentSimilarity);
        // end of loop
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

    private void calculateMaximalDistance(Connection dbConnection) {
        if (dbConnection != null) {
            try {
                CallableStatement callableStatement = dbConnection.prepareCall("{? = call MaximalDistance}");
                callableStatement.execute();
                callableStatement.registerOutParameter(1, Types.INTEGER);
                this.maxDistance = callableStatement.getInt(1);
            }
            catch (SQLException throwable) {
                throwable.printStackTrace();
            }
        }
    }

    private List<Long> getAllMovieIDs(Connection dbConnection) {
        return null;


    }

    private float getSingleSimilarity(Connection dbConnection, long mid1, long mid2) {
        try {
            CallableStatement callableStatement = dbConnection.prepareCall("");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    private void writeSingleSimilarity(Connection dbConnection, long mid1, long mid2, float similarity) {
        try {
            CallableStatement callableStatement = dbConnection.prepareCall("");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }


}
