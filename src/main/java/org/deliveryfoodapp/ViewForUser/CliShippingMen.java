package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;

public class CliShippingMen
{
    public static void cliShippingMen(String nickname)
    {
        boolean exit = false;
        int input;
        JFrame frame = new JFrame();

        //System.out.println("Choose a operation:");
        //System.out.println("1)Do an order");
        //System.out.println("2)Shows your orders");
        //System.out.println("3)Show your data");
        //System.out.println("4)Exit");

        Object[] options = {"Notify a Shipping",
                "Exit"};

        //Ask to the shipping men to choose a operation
        while (!exit)
        {
            input = JOptionPane.showOptionDialog(frame,
                    "Choose a operation",
                    "Welcome Shipping Men " + nickname,
                    JOptionPane.YES_NO_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[1]);

            switch (input)
            {
                case 0:
                    //Create the gui to notify a shipping not complete
                    CreateNotifyShippingView.showShippingNotCompleteView(nickname);
                    break;
                case 1:
                    return;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
            }
        }
    }
}
