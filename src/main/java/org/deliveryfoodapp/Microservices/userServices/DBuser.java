package org.deliveryfoodapp.Microservices.userServices;


import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBuser
{
    private String pathFile = "/home/cysko7927/IdeaProjects/middleware_spark_complete/FoodDeliveryApplication/DB/user.txt";

    /**
     * Write the credentials of the new user if there aren't in the DB
     * @param username
     * @param password
     * @param type
     */
    public void writeCredentialsUser(String username,String password,String type)
    {
        List<String> list;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Check if there is a user in the DB with the username in input
        {


            list = stream
                    .filter(line -> line.startsWith(username))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return;
        }

        FileWriter fw;

        if (list.isEmpty())//If there isn't a user with the username in input
        {
            try
            {
                fw = new FileWriter(pathFile, true);
                fw.write(username+ "," + password + "," + type + "\n");//Write the credentials in the DB
                fw.close();



            } catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("Error in reading the user DB");
                return;
            }

        }


    }

    /**
     * Search the users that have the username and the password in input
     * @param username
     * @return list that contains the credentials of the users with the username and password in input
     */
    public List<String> searchUser(String username)
    {
        List<String> list = new ArrayList<>();

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Check if there is a user with the username and password in input
        {


            list = stream
                    .filter(line -> line.startsWith(username+","))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return list;
        }

        return list;

    }

    public int updateAddressShipping(String nickname,String address)
    {
        try {
            Path path = Paths.get(pathFile);
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8); //Read and save all lines
            int lineNumber = 0;

            for (int i = 0; i < lines.size(); i++) //Obtanin the index of the line to modify the address
            {
                if (lines.get(i).contains(nickname+","))
                    lineNumber = i;
            }

            String[] dataUser = lines.get(lineNumber).split(",");
            lines.set(lineNumber, dataUser[0]+","+dataUser[1]+","+dataUser[2] +","+ address);//modify the line
            Files.write(path, lines, StandardCharsets.UTF_8);//Write all the lines in the files

        } catch (Exception e) {
            System.out.println("Problem reading file Item.");
            return 1;
        }

        return 0;
    }
}
