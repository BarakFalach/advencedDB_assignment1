public class Main {
    public static void main(String[] args){

        DataBaseConnection dbConnection = new DataBaseConnection("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE", "yitzhakh", "abcd");

        dbConnection.fileToDataBase("assets/films.csv");

    }
}
