package org.deliveryfoodapp.ViewForUser;

import org.deliveryfoodapp.broker.NetworkBroker;

public class Connect {

    private int PORT;
    private String HOSTNAME = "localhost";

    public Connect()
    {
        PORT = Integer.parseInt(NetworkBroker.serverAuth.split(":")[1]);
        HOSTNAME = NetworkBroker.serverAuth.split(":")[0];

    }

    public int getPort(){
        return this.PORT;
    }

    public String gethostName(){
        return this.HOSTNAME;
    }
}