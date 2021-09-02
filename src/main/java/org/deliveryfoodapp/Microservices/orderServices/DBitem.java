package org.deliveryfoodapp.Microservices.orderServices;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBitem {

    private String pathFile = "/home/cysko7927/IdeaProjects/middleware_spark_complete/FoodDeliveryApplication/DB/items.txt";

    public DBitem() throws IOException {
        List<String> lines = Files.lines(Paths.get("./configDBorderAndItem.txt")).collect(Collectors.toList());
        this.pathFile = lines.get(0);
    }

    public String obtainAllitems()
    {
        try {
            return Files.readString(Paths.get(pathFile));//Read all the file and return all the string;
        } catch (IOException e)
        {
            e.printStackTrace();
            return "Error";
        }
    }
    public int obtainQuantity(String item)
    {
        List<String> items;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))
        {
            //The line are like this: itemx,quantity

            items = stream
                    .filter(line -> line.startsWith(item)) //Obtain the item requested
                    .collect(Collectors.toList());

        } catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return 0;
        }

        String[] itemAndQuantity = items.get(0).split(",");//Split item and quantity

        return Integer.parseInt(itemAndQuantity[1]);// returns quantity
    }

    public void modifyQuantity(String item,int quantity) {
        List<String> items;

        try (Stream<String> stream = Files.lines(Paths.get(pathFile)))//Obtain the line with the requested item
        {
            //The line are like this: itemx,quantity

            items = stream
                    .filter(line -> line.contains(item+",")) //Obtain the item requested
                    .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error in reading the user DB");
            return;
        }

        if (items.size() == 1) { //If the item requested is in the DB

            try {
                Path path = Paths.get(pathFile);
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8); //Read and save all lines
                int lineNumber = 0;

                for (int i = 0; i < lines.size(); i++) //Obtanin the index of the line to modify to update the quantity of the item
                {
                    if (lines.get(i).contains(item + ","))
                        lineNumber = i;
                }

                lines.set(lineNumber, item + "," + quantity);//modify the line with the new quantity
                Files.write(path, lines, StandardCharsets.UTF_8);//Write all the lines in the files

            } catch (Exception e) {
                System.out.println("Problem reading file Item.");
            }

        }
        else //If there isn't the  item
        {
            FileWriter fw;


            try
            {
                fw = new FileWriter(pathFile, true);
                fw.write(item+","+quantity + "\n");//Add it to the DB
                fw.close();



            } catch (IOException e)
            {
                e.printStackTrace();
                System.out.println("Error in reading the user DB");
            }
        }
    }
}
