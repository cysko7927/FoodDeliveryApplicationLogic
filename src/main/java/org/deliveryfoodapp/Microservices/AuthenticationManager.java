package org.deliveryfoodapp.Microservices;

import org.deliveryfoodapp.Microservices.userServices.DBuser;
import org.deliveryfoodapp.ViewForUser.Connect;

import java.net.*;
import java.io.*;
import java.util.List;

public class AuthenticationManager {
    private int currentTot;
    ServerSocket serversocket;
    Socket client;
    int bytesRead;
    Connect c = new Connect();
    BufferedReader input;
    PrintWriter output;
    DBuser dBuser = new DBuser();

    public AuthenticationManager() throws IOException
    {
        System.out.println("Connection Starting on port:" + c.getPort());
        //make connection to client on port specified
        serversocket = new ServerSocket(c.getPort());
    }

    public void start() throws IOException{

        System.out.println("Waiting for connection from client");
        //accept connection from client
        client = serversocket.accept();



        try {
            logInfo();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void logInfo() throws Exception{
        //open buffered reader for reading data from client
        input = new BufferedReader(new InputStreamReader(client.getInputStream()));

        String username = input.readLine();
        System.out.println("SERVER SIDE:" + username);
        String password = input.readLine();
        System.out.println("SERVER SIDE:" + password);

        //open printwriter for writing data to client
        output = new PrintWriter(new OutputStreamWriter(client.getOutputStream()));

        List<String> usersCredentials = dBuser.searchUser(username);
        if(!usersCredentials.isEmpty()){
            output.println(extractTypeUserFromRowDb(usersCredentials.get(0)));
        }else{
            output.println("DENIED");
        }

        output.flush();
        client.close();

    }

    private String extractTypeUserFromRowDb(String row)
    {
        if (row.contains("CUSTOMER"))
            return "CUSTOMER";
        if (row.contains("ADMIN"))
            return "ADMIN";
        if (row.contains("SHIPPINGMEN"))
            return "SHIPPINGMEN";

        return "DENIED";
    }

    public static void main(String[] args) {
        boolean error = false;

        AuthenticationManager authenticationManager = null;
        try {
            authenticationManager = new AuthenticationManager();
        } catch (IOException e) {
            error = true;
            System.out.println("Error of creation for the socket");
        }


        while (!error)
        {
            try {
                authenticationManager.start();
            }
            catch (IOException e){
                error = true;
                System.out.println("Error of connection");
            }
        }
    }

}