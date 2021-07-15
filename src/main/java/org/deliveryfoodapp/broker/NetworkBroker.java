package org.deliveryfoodapp.broker;

public class NetworkBroker
{
    static public String server0 = "localhost:9092";
    static public String server1 = "localhost:9093";
    static public String server2 = "localhost:9094";
    static public String serverAuth = "localhost:9090";
    private static boolean isServer1 = false;
    static  public String actualserver = "localhost:9092";
    public static void switchServer()
    {
        isServer1 = !isServer1;

        if (isServer1)
        {
            actualserver = server1;
        }
        else
        {
            actualserver = server0;
        }
    }
}
