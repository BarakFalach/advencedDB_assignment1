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
    private String GET_SIMILARITY;
    private String MAX_DISTANCE;
    private final String INSERT_SIMILARITY;

    public DataBaseConnection(String connectionString, String userName, String password) {
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
        this.maxDistance = 0;
        this.GET_SIMILARITY = "{? = call SimCalculation(?, ?, ?)}";
        this.MAX_DISTANCE = "{? = call MaximalDistance}";
        this.INSERT_SIMILARITY = "INSERT INTO SIMILARITY VALUES (?, ?, ?)";
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
            dbConnection.close();
        }
        catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void calculateSimilarity() throws SQLException {
        List<Long> movieIds = new ArrayList<>();
        Connection dbConnection = createConnection();
        long mid1;
        long mid2;
        float currentSimilarity = 0;
        if (dbConnection == null) return;
        calculateMaximalDistance(dbConnection);
        movieIds = getAllMovieIDs(dbConnection);
        assert movieIds != null;
        for (int i = 0 ; i < movieIds.size() ; i ++) {
            for (int j = i + 1 ; j < movieIds.size() ; j ++) {
                mid1 = movieIds.get(i);
                mid2 = movieIds.get(j);
                currentSimilarity = getSingleSimilarity(dbConnection,mid1,mid2);
                writeSingleSimilarity(dbConnection, mid1, mid2, currentSimilarity);
            }
        }
        dbConnection.close();
    }

    public void printSimilarItems(long mid) {
        Connection dbConnection = createConnection();
        //TODO:: write  sql query.
        String Query = "SELECT * Ftom";
        PreparedStatement preparedStatement;
        ResultSet resultSet;
        try {
            assert dbConnection != null;
            preparedStatement = dbConnection.prepareStatement(Query);
            //TODO:: insert Parameters to query, for every '?' in query;
            // for example: if you want to insert the first parameter of type long,
            // statement.setLong(1, 1240215641234);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                System.out.println("MID: " + resultSet.getString(1));
                System.out.println("Similarity: " + resultSet.getFloat(2));
            }


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


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
                CallableStatement callableStatement = dbConnection.prepareCall(this.MAX_DISTANCE);
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
        String selectMIds = "Select MID FROM MEDIAITEMS";
        try {
            Statement statement = dbConnection.createStatement();
            ResultSet dataFromDb = statement.executeQuery(selectMIds);
            List<Long> mIdArray = new ArrayList<>();
            while (dataFromDb.next()){
                mIdArray.add(dataFromDb.getLong(1));
            }
            return mIdArray;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    private float getSingleSimilarity(Connection dbConnection, long mid1, long mid2) {
        try {
            CallableStatement callableStatement = null;
            callableStatement = dbConnection.prepareCall(this.GET_SIMILARITY);
            callableStatement.registerOutParameter(1, Types.FLOAT);
            callableStatement.setLong(2, mid1);
            callableStatement .setLong(3, mid2);
            callableStatement.setInt(4, this.maxDistance);
            callableStatement.execute();
            return callableStatement.getFloat(1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    private void writeSingleSimilarity(Connection dbConnection, long mid1, long mid2, float similarity) {
        try {
            PreparedStatement preparedStatement;
            preparedStatement = dbConnection.prepareStatement(this.INSERT_SIMILARITY);
            preparedStatement.setString(1, String.valueOf(mid1));
            preparedStatement.setString(2, String.valueOf(mid2));
            preparedStatement.setString(3, String.valueOf(similarity));
            preparedStatement.executeQuery();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }


}
