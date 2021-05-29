package org.deliveryfoodapp.Microservices.shippingServices;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbShipping {

    private String pathFile = "/home/cysko7927/IdeaProjects/middleware_spark_complete/FoodDeliveryApplication/DB/shipment.txt";

    public String obtainAllShipmentNotCompleted()
    {
        List<String> list;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain all the row with the orders of the user with the nickname in input
        {


            list = stream
                    .filter(line -> line.contains("notDelivered"))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return "Error";
        }


        String allShipment = "";

        for (String line:list)
        {
            allShipment = allShipment + line + "\n";
        }

        return allShipment;
    }
    public void addShipment(String nickname ,String key)
    {
        List<String> list= new ArrayList<>();

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Check if the shipment is already here
        {


            list = stream
                    .filter(line -> line.contains(key+","+nickname))
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
        }


        if (list.size() == 0) //If there isn't the shipment
        {
            FileWriter fw;


            try
            {
                fw = new FileWriter(pathFile, true);
                fw.write(key+ "," + nickname + "," + "notDelivered" +"\n");//Write the shipment not delivered in the DB
                fw.close();



            } catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("Error in reading the user DB");

            }


        }




    }

    public int completeAShipment(String key,String nickname)
    {
        List<String> shipments;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain the line with the requested item
        {
            //The line are like this: key,nickname,stateShipment

            shipments = stream
                    .filter(line -> line.contains(key+","+nickname+",notDelivered")) //Obtain the item requested
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return 2; //Error reading DB
        }

        if (shipments.size() == 1) { //If the item requested is in the DB

            try {
                Path path = Paths.get(pathFile);
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8); //Read and save all lines
                int lineNumber = 0;

                for (int i = 0; i < lines.size(); i++) //Obtanin the index of the line to modify to update the quantity of the item
                {
                    if (lines.get(i).contains(key+","+nickname))
                        lineNumber = i;
                }

                lines.set(lineNumber, key+","+nickname+",Completed");//modify the line
                Files.write(path, lines, StandardCharsets.UTF_8);//Write all the lines in the files

            } catch (Exception e) {
                System.out.println("Problem reading file shipment.");
                return 2;//Error reading DB
            }

            return 0;//Shipment completed

        }
        else
        {
            return 1;//Shipment yet completed
        }

    }
}
