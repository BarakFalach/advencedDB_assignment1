public class Main {
    public static void main(String[] args){

        DataBaseConnection dbConnection = new DataBaseConnection("String", "Barak", "8995");

        dbConnection.fileToDataBase("assets/films.csv");

    }
}
