package org.deliveryfoodapp.ViewForUser;

import org.apache.kafka.common.protocol.types.Field;
import org.deliveryfoodapp.Model.TypeOfUser;
import org.deliveryfoodapp.Model.User;

import javax.swing.*;

public class RegisterUser
{
    private static String username = "";
    private static String password = "";
    private static TypeOfUser type;

    public static void registerUser()
    {
        String input;
        JFrame frame = new JFrame();

        input = (String) JOptionPane.showInputDialog(
                frame,
                "Insert the nickname",
                "Registration",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        //TODO Check if the nickname is available

        username = new String(input);


        System.out.println("Insert the password:");
        input = (String) JOptionPane.showInputDialog(
                frame,
                "Insert the password",
                "Registration",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        password = new String(input);

        System.out.println("Chose the type of the user:");
        System.out.println("1)Customer");
        System.out.println("2)Admin");
        System.out.println("3)ShippingMen");

        Object[] options = {"Customer",
                "Admin",
                "ShippingMen"};

        int choose = JOptionPane.showOptionDialog(
                frame,
                "Chose the type of the user",
                "Registration",
                JOptionPane.YES_NO_CANCEL_OPTION,
                JOptionPane.QUESTION_MESSAGE,
                null,
                options,
                options[0]);

        boolean done =false;

        while (!done)
        {
            switch (choose)
            {
                case 0:
                    type = TypeOfUser.CUSTOMER;
                    done = true;
                    break;
                case 1:
                    type = TypeOfUser.ADMIN;
                    done =true;
                    break;
                case 2:
                    type =TypeOfUser.SHIPPINGMEN;
                    done = true;
                    break;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
                    choose = JOptionPane.showOptionDialog(
                            frame,
                            "Chose the type of the user",
                            "Registration",
                            JOptionPane.YES_NO_CANCEL_OPTION,
                            JOptionPane.QUESTION_MESSAGE,
                            null,
                            options,
                            options[0]);

            }
        }

        UserProducer.sendRegistrationAtBroker(username,password,type);//Send the data at the producer of the user


    }
}
