package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;

/**
 * This class handles the GUI with the MainMenu of a customer
 */
public class CliCustomer
{

    public static void cliCustomer(String nickname)
    {
        boolean exit = false;
        int input;
        JFrame frame = new JFrame();

        //System.out.println("Choose a operation:");
        //System.out.println("1)Do an order");
        //System.out.println("2)Shows your orders");
        //System.out.println("3)Show your data");
        //System.out.println("4)Exit");

        Object[] options = {"Do an order",
                "Show your orders",
                "Show your data",
                "Set your Address",
                "Exit"};
        //Ask to the customer to choose a operation
        while (!exit)
        {
            input = JOptionPane.showOptionDialog(frame,
                    "Choose a operation",
                    "Welcome customer " + nickname,
                    JOptionPane.YES_NO_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[3]);

            switch (input)
            {
                case 0:
                    //Create the Gui to make an order with the CreateOrderView
                    CreateOrderView.executeOrderViewCreation(nickname);
                    break;
                case 1:
                    //Shows orders done by user with the CreateOrderView
                    CreateOrderView.executeOrdersView(nickname);
                    break;
                case 2:
                    //Show data User(Nick,password,Address)
                    UserDataView.showUserDataView(nickname);
                    break;
                case 3:
                    //Show a GUI to insert the new ShippingAddress
                    UserDataView.setAddressView(nickname);
                    break;
                case 4:
                    exit = true;
                    break;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
            }
        }
    }
}
