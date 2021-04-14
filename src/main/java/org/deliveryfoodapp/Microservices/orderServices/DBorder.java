package org.deliveryfoodapp.Microservices.orderServices;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBorder
{
    private String pathFile = "/home/cysko7927/IdeaProjects/middleware_spark_complete/FoodDeliveryApplication/DB/orders.txt";

    public String  obtainAllOrdersOfAUser(String nickname)
    {
        List<String> list;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain all the row with the orders of the user with the nickname in input
        {


            list = stream
                    .filter(line -> line.contains(nickname))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return "Error";
        }

        String allOrder = "";

        for (String line:list)
        {
            allOrder = allOrder + line + "\n";
        }

        return allOrder;
    }
    public int addOrder(String allItems ,String nickname)
    {
        List<String> list;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain all the row with the orders of the user with the nickname in input
        {


            list = stream
                    .filter(line -> line.contains(nickname))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return 0;
        }

        int index = 0;
        if (list.size() == 0) //If there aren't orders of the user
            index = 1; //Set the key of the order to 1
        else
            index = list.size() + 1;//Else increment the key of the last order done by the user and set as new key for the new order

        FileWriter fw;


            try
            {
                fw = new FileWriter(pathFile, true);
                fw.write(index+ "," + nickname + "," + "notDelivered" +"," + allItems + "\n");//Write the credentials in the DB
                fw.close();



            } catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("Error in reading the user DB");
                return 0;
            }

        return index;

    }
}
