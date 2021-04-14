package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;
import java.io.IOException;

public class LoginUser {
    private static String username = "";
    private static String password = "";


    public static void login()
    {
        JFrame frame = new JFrame();
        String input;

        input = (String) JOptionPane.showInputDialog(
                frame,
                "Insert the nickname",
                "Login",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        username = new String(input);



        input = (String) JOptionPane.showInputDialog(
                frame,
                "insert the password",
                "Login",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        password = new String(input);

        LoginHandler clientLogin = new LoginHandler();

        String response;
        try
        {
            response = clientLogin.startClient(username,password);

        } catch (IOException e)
        {
            JOptionPane.showMessageDialog(frame,"error of connection");
            e.printStackTrace();
            return;
        }


        switch(response)
        {
            case "DENIED":
                JOptionPane.showMessageDialog(frame,"Wrong credentials");
                break;
            case "CUSTOMER":
                System.out.println("Welcome customer " + username);
                //Create Cli for Customer
                CliCustomer.cliCustomer(username);
                break;
            case "SHIPPINGMEN":
                System.out.println("Welcome Shipping men " + username);
                //Create Cli for ShippingMen todo
                break;
            case "ADMIN":
                System.out.println("Welcome Admin " + username);
                //Create Cli for Admin
                CliAdmin.cliAdmin(username);
                break;
            default:
                JOptionPane.showMessageDialog(frame,"Error in the server");

        }


    }
}
