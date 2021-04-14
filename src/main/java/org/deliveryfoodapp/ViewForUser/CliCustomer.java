package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;

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
                "Exit"};

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
                    //CreateOrderView
                    CreateOrderView.executeOrderViewCreation(nickname);
                    break;
                case 1:
                    //Shows orders with the CreateOrderView
                    CreateOrderView.executeOrdersView(nickname);
                    break;
                case 2:
                    //Show data User
                    return;
                case 3:
                    return;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
            }
        }
    }
}
