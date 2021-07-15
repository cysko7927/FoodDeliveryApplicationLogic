package org.deliveryfoodapp.ViewForUser;
import org.apache.tomcat.util.buf.StringUtils;

import javax.swing.*;
import java.util.*;
public class InputReader
{
    public static String readString()
    {
        Scanner sc= new Scanner(System.in); //System.in is a standard input stream

        String str= sc.nextLine();
        //reads string


        return str;

    }
    public static int readIntBeetween(int min,int max,String text)
    {

        JFrame frame = new JFrame();
        String str= (String) JOptionPane.showInputDialog(
                frame,
                text,
                "Insert the index of the item to purchase(insert 0 to terminate)",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        while(!(str != null && str.matches("-?\\d+(\\.\\d+)?") && Integer.parseInt(str) >= min && Integer.parseInt(str) <= max))
        {
            JOptionPane.showMessageDialog(frame,
                    "index is not valid");
            str = (String) JOptionPane.showInputDialog(
                    frame,
                    text,
                    "Insert the index of the item to purchase(insert 0 to terminate)",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");
        }

        return Integer.parseInt(str);

    }
    public static int readIntGreaterOrEqualOf1()
    {

        JFrame frame = new JFrame();
        String str= (String) JOptionPane.showInputDialog(
                frame,
                "Insert the quantity to purchase",
                "Insert quantity",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        while(!(str != null && str.matches("-?\\d+(\\.\\d+)?") && Integer.parseInt(str) >= 1))
        {
            JOptionPane.showMessageDialog(frame,
                    "quantity is not valid");
            str = (String) JOptionPane.showInputDialog(
                    frame,
                    "Insert the quantity to purchase",
                    "Insert quantity",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");
        }

        return Integer.parseInt(str);

    }

    public static String obtainAnAddress()
    {

        JFrame frame = new JFrame();
        String str= (String) JOptionPane.showInputDialog(
                frame,
                "Insert the address",
                "Modify address",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        while(!(str != null && !str.contains(",")))
        {
            JOptionPane.showMessageDialog(frame,
                    "Address is not valid");
            str = (String) JOptionPane.showInputDialog(
                    frame,
                    "Insert the address",
                    "Modify address",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    "");
        }

        return str;
    }
}
