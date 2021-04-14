package org.deliveryfoodapp.ViewForUser;

import java.io.*;
import java.net.*;

/**
 * This class implements a client Socket that send the username and the password to
 * the server to authenticate the user
 */
public class LoginHandler
{
    private final String FILENAME = null;
    Connect c = new Connect();
    Socket socket;
    BufferedReader read;
    PrintWriter output;

    public String startClient(String username,String password) throws UnknownHostException, IOException{
        //Create socket connection
        socket = new Socket(c.gethostName(), c.getPort());

        //create printwriter for sending login to server
        output = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));


        //send user name to server
        output.println(username);

        //send password to server
        output.println(password);
        output.flush();

        //create Buffered reader for reading response from server
        read = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        //read response from server
        String response = read.readLine();



        return response;

    }

    public void fileInfo(){

    }


}
