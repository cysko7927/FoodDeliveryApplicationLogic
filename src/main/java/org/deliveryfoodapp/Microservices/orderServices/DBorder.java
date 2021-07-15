package org.deliveryfoodapp.Microservices.orderServices;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public int addOrder(String addressAndAllItems ,String nickname)
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
                fw.write(index+ "," + nickname + "," + "notDelivered" +"," + addressAndAllItems  + "\n");//Write the credentials in the DB
                fw.close();



            } catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("Error in reading the user DB");
                return 0;
            }

        return index;

    }


    public void changeStateOrder(String keyOrder,String nickUser)
    {
        List<String> orders;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain the line with the requested item
        {
            //The line are like this: keyOrder,nickUser,....

            orders = stream
                    .filter(line -> line.contains(keyOrder+","+nickUser+",")) //Obtain the item requested
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return;
        }

        if(orders.size() == 0)
            return;//If there isn't the order to complete then do nothing

        try {
            Path path = Paths.get(pathFile);
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8); //Read and save all lines
            int lineNumber = 0;

            for (int i = 0; i < lines.size(); i++) //Obtanin the index of the line to modify to update the quantity of the item
            {
                if (lines.get(i).contains(keyOrder+","+nickUser+","))
                    lineNumber = i;
            }

            String address = lines.get(lineNumber).split(",")[3]; // obtain the address
            String[] items = lines.get(lineNumber).split(keyOrder+","+nickUser+","+"notDelivered,"+address+",");//Obtain all the items in the order
            lines.set(lineNumber, keyOrder+","+nickUser+","+"Completed"+","+address + ","+ items[1]);//modify the line
            Files.write(path, lines, StandardCharsets.UTF_8);//Write all the lines in the files

        } catch (Exception e) {
            System.out.println("Problem reading file Item.");
        }
    }
}
