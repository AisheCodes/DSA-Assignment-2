import ballerina/io;
import ballerina/time;

type OrderItem record {|
    string name;
    int quantity;
    decimal price;
|};

type Order record {|
    int orderId;
    int customerId;
    int restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    time:Utc createdAt;
    time:Utc updatedAt;
|};

function create_order(
    int orderId,
    int customerId,
    int restaurantId,
    OrderItem[] items
) returns Order {

    decimal totalAmount = 0;

    foreach OrderItem item in items {
        totalAmount += item.price * item.quantity;
    }

    time:Utc now = time:utcNow();

    return {
        orderId: orderId,
        customerId: customerId,
        restaurantId: restaurantId,
        items: items,
        totalAmount: totalAmount,
        status: "CREATED",
        createdAt: now,
        updatedAt: now
    };
}

public function main() {
    OrderItem[] items = [
        {name: "Chicken Burger", quantity: 2, price: 90.00},
        {name: "Fries", quantity: 1, price: 30.00}
    ];

    Order newOrder = create_order(
        1,
        101,
        201,
        items
    );

    io:println(newOrder);
}