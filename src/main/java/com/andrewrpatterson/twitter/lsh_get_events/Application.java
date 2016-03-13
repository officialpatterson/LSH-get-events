package com.andrewrpatterson.twitter.lsh_get_events;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by apatterson on 08/03/2016.
 */
public class Application {
    Connection connection;
    static final double ENTROPY_threshold = 3.5;
    List<String> events = new ArrayList<String>();
    public Application(){
        try {

            connection = DriverManager.getConnection("jdbc:postgresql://130.209.244.112:5432/andrew", "andrew", "andrew");

        } catch (SQLException e) {
            System.out.println("Unable to connect to database");
            connection = null;

        }
    }
    public Set<Long> getThreadIDs(){
        Statement stmt = null;
        String query = "select * from events";
        Set<Long> events = new HashSet<Long>();

        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next())
                events.add(rs.getLong("thread"));


        } catch (SQLException e ) {
           return null;
        } finally {
            if (stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return events;
    }

    public void outputEvents(){

        //1. get the thread ids
        Set<Long> eventIDs = getThreadIDs();

        System.out.println("Number of clusters: "+eventIDs.size());


        int i = 0;
        //2. Perform a query to get set of tweets for each event ID
        for(Long thread: eventIDs){
            processEvent(thread);
            i++;

            if(i%10000 ==0)
                System.out.println(i+" processed");
        }
        

        //output events from eventSet
        try {
            Files.write(Paths.get("events.csv"),events);

            PrintWriter writer = new PrintWriter("events.csv", "UTF-8");

            for(String e: events){
                writer.println(e);
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processEvent(long thread){

        try {
            PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM tweet where thread=?");

            //1. get the set of tweets and place id, tweet pairs into map
            List<String> rows = new ArrayList<String>();
            pstmt.setLong(1, thread);
            ResultSet result = pstmt.executeQuery();
            List<List> tokenSet = new ArrayList<List>();

            while (result.next()) {
                rows.add(result.getLong("id") + ", " + result.getString("text"));

                //get the tokens, split them, and add to the tokens array
                List<String> items = Arrays.asList(result.getString("tokens").split("\\s*"));
                tokenSet.add(items);
            }



            //3. if the entropy is greater than ENTROPY_THRESHOLD then output to csv
            if( Application.calculateShannonEntropy(tokenSet) >= ENTROPY_threshold){
                //output to csv

                System.out.println("Found event");
                events.add(Long.toString(thread));

            }
        }catch(SQLException e){
            System.out.println("Unable to process event due to SQLException");
            System.out.println(e);
        }

    }

    public static void main(String[] args){

        System.out.print("Connecting to Database...");
        Application application = new Application();
        System.out.println("Done");

        if(application != null)
        application.outputEvents();
    }
    public static Double calculateShannonEntropy(List<List> documents) {

        //1. create TF table
        Map<String, Long>  termFrequencies = new HashMap<String, Long>();

        //number of words
        long numberOfTerms = 0;

        for(List<String> tokens: documents)
            for(String token: tokens){
                if(termFrequencies.containsKey(token))
                    termFrequencies.put(token, termFrequencies.get(token)+1);
                else
                    termFrequencies.put(token, 1l);

                numberOfTerms++;
            }

        //now for each term, calculate its probability of occurring
        double entropy = 0;
        Iterator it = termFrequencies.entrySet().iterator();


        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();


            double prob =  (Long)pair.getValue()/(double)numberOfTerms; //one of the operands has to be a double
            entropy = entropy+ prob*Math.log(prob);

        }

        return entropy*-1;
    }

}
