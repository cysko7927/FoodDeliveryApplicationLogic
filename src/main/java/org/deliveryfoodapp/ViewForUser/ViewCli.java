package org.deliveryfoodapp.ViewForUser;

import javax.swing.*;

public class ViewCli
{
    static public void mainMenu()
    {
        //System.out.println("Choose a operation:");
        //System.out.println("1)Registration:");
        //System.out.println("2)Login:");
        //System.out.println("3)Exit:");

        JFrame frame = new JFrame();

        Object[] options = {"Registration",
                "Login",
                "Exit"};

        boolean finish = false;

        while (!finish)
        {
            //Ask to the user to choose a operation
            int op = JOptionPane.showOptionDialog(frame,
                    "Choose a operation",
                    "Welcome User ",
                    JOptionPane.YES_NO_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE,
                    null,
                    options,
                    options[2]);

            switch (op) //check the choose of the user
            {
                case 0:
                    RegisterUser.registerUser();
                    break;
                case 1:
                    LoginUser.login();
                    break;
                case 2:
                    finish = true;
                    break;
                default:
                    JOptionPane.showMessageDialog(frame,"input is not valid");
            }
        }

    }
}
