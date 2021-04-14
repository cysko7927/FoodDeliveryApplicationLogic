package org.deliveryfoodapp.Model;

import java.util.List;

public class Customer extends User
{
    private List<Order> orders;
    private List<AddressOfShipment> address;
}
