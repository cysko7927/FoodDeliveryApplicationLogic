package org.deliveryfoodapp.Microservices.userServices;

public class Credentials
{
    String username;
    String password;
    String type;

    public Credentials(String username, String password, String type) {
        this.username = username;
        this.password = password;
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getType() {
        return type;
    }
}
