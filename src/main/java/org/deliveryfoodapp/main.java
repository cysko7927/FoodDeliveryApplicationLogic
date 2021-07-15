package org.deliveryfoodapp;

import org.deliveryfoodapp.ViewForUser.ViewCli;
import org.deliveryfoodapp.broker.NetworkBroker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class main
{
    public static void main(String[] args) throws IOException {
        //Reading and set the address for the brokers
        List<String> servers = Files.lines(Paths.get("./config.txt")).collect(Collectors.toList());
        NetworkBroker.server0 = servers.get(0).split("=")[1];
        NetworkBroker.server1 = servers.get(1).split("=")[1];
        NetworkBroker.server2 = servers.get(2).split("=")[1];
        NetworkBroker.serverAuth = servers.get(3).split("=")[1];


        ViewCli.mainMenu();

    }

}
