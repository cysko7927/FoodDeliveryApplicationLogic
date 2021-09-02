package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;

public class CliAdmin {

    public static void cliAdmin(String nickname)
    {
        boolean exit = false;
        int input;

       // System.out.println("Choose a operation:");
        //System.out.println("1)Show items");
        //System.out.println("2)Insert new item/Modify quantity");
        //System.out.println("3)Exit");

        Object[] options = {"Show items",
                "Insert new item/Modify quantity",
                "Exit"};
        JFrame frame = new JFrame();

        //Ask to the user to choose a operation
        while (!exit)
        {
            input = JOptionPane.showOptionDialog(frame,
                    "Choose a operation",
                    "Welcome Admin " + nickname,
                    JOptionPane.YES_NO_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[2]);

            switch (input)
            {
                case 0:
                    //Show all item in the Warehouse
                    HandleItemsView.showItemView(nickname);
                    break;
                case 1:
                    //Insert a new item/Modify quantity
                    HandleItemsView.addItemView(nickname);
                    break;
                case 2:
                    return;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
            }
        }
    }
}
