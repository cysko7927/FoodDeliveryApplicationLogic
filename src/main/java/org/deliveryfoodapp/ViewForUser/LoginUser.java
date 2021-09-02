package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;
import java.io.IOException;

/**
 * This Class handles the GUI for the login of the user
 *
 */
public class LoginUser {
    private static String username = "";
    private static String password = "";


    public static void login()
    {
        JFrame frame = new JFrame();
        String input;

        //Ask the nick to the user
        input = (String) JOptionPane.showInputDialog(
                frame,
                "Insert the nickname",
                "Login",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        if (input == null)
            return;

        username = new String(input);


        //Ask the password to the user
        input = (String) JOptionPane.showInputDialog(
                frame,
                "insert the password",
                "Login",
                JOptionPane.PLAIN_MESSAGE,
                null,
                null,
                "");

        if (input == null)
            return;
        password = new String(input);

        LoginHandler clientLogin = new LoginHandler();

        String response;
        try
        {
            response = clientLogin.startClient(username,password); //Send the credentials to the authentication Manager

        } catch (IOException e)
        {
            JOptionPane.showMessageDialog(frame,"error of connection");
            e.printStackTrace();
            return;
        }


        switch(response) //Check the response
        {
            case "DENIED":
                JOptionPane.showMessageDialog(frame,"Wrong credentials");
                break;
            case "CUSTOMER":
                System.out.println("Welcome customer " + username);
                //Create GUI for Customer
                CliCustomer.cliCustomer(username);
                break;
            case "SHIPPINGMEN":
                System.out.println("Welcome Shipping men " + username);
                //Create GUI for ShippingMen
                CliShippingMen.cliShippingMen(username);
                break;
            case "ADMIN":
                System.out.println("Welcome Admin " + username);
                //Create GUI for Admin
                CliAdmin.cliAdmin(username);
                break;
            default:
                JOptionPane.showMessageDialog(frame,"Error in the server");

        }


    }
}
