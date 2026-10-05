import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

type Parties record {
    string customer_id = "";
    string restaurant_id = "";
    string driver_id = "";
};

type NotificationRow record {|
    int id;
    string recipient_type;
    string recipient_id;
    string channel;
    string order_id;
    string message;
    string created_at;
|};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        recipient_type TEXT NOT NULL,
        recipient_id TEXT NOT NULL,
        channel TEXT NOT NULL,
        order_id TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL)`);
}

// Simulates sending (email / sms / push / dashboard): stores it and logs it
function notify(string rtype, string rid, string channel, string orderId, string msg) returns error? {
    check exec(`INSERT INTO notifications (recipient_type, recipient_id, channel, order_id, message, created_at)
        VALUES (${rtype}, ${rid}, ${channel}, ${orderId}, ${msg}, ${nowStr()})`);
    log:printInfo("NOTIFY [" + channel + "] " + rtype + " " + rid + ": " + msg);
}

function handleEvent(Event ev) returns error? {
    Parties p = check ev.payload.cloneWithType();
    string o = ev.orderId;
    match ev.'type {
        "ORDER_CREATED" => {
            check notify("restaurant", p.restaurant_id, "dashboard", o, "New order " + o + " received");
            check notify("customer", p.customer_id, "email", o, "We received your order " + o);
        }
        "RESTAURANT_REJECTED" => {
            check notify("customer", p.customer_id, "push", o, "Sorry, the restaurant could not accept order " + o);
        }
        "PAYMENT_COMPLETED" => {
            check notify("customer", p.customer_id, "sms", o, "Payment received for order " + o);
        }
        "PAYMENT_FAILED" => {
            check notify("customer", p.customer_id, "email", o, "Payment failed for order " + o + ". Order cancelled.");
        }
        "ORDER_CONFIRMED" => {
            check notify("restaurant", p.restaurant_id, "dashboard", o, "Order " + o + " is paid - start preparing");
            check notify("customer", p.customer_id, "push", o, "Order " + o + " confirmed");
        }
        "ORDER_PREPARING" => {
            check notify("customer", p.customer_id, "push", o, "The kitchen is preparing order " + o);
        }
        "ORDER_READY" => {
            check notify("customer", p.customer_id, "push", o, "Order " + o + " is ready. Finding a driver.");
        }
        "DRIVER_ASSIGNED" => {
            check notify("driver", p.driver_id, "sms", o, "Pick up order " + o);
            check notify("customer", p.customer_id, "push", o, "Driver " + p.driver_id + " assigned to order " + o);
        }
        "DELIVERY_PICKED_UP" => {
            check notify("customer", p.customer_id, "push", o, "Order " + o + " is out for delivery");
        }
        "DELIVERY_COMPLETED" => {
            check notify("customer", p.customer_id, "email", o, "Order " + o + " delivered. Enjoy!");
            check notify("restaurant", p.restaurant_id, "dashboard", o, "Order " + o + " was delivered");
            check notify("driver", p.driver_id, "sms", o, "Delivery of order " + o + " completed");
        }
        "ORDER_CANCELLED" => {
            check notify("customer", p.customer_id, "email", o, "Order " + o + " was cancelled");
            check notify("restaurant", p.restaurant_id, "dashboard", o, "Order " + o + " was cancelled");
        }
    }
}

listener http:Listener api = new (httpPort);

service /notifications on api {

    resource function get .() returns json|error {
        stream<NotificationRow, sql:Error?> rs = db->query(`SELECT * FROM notifications ORDER BY id DESC LIMIT 100`);
        NotificationRow[] rows = check from NotificationRow r in rs
            select r;
        return rows.toJson();
    }

    // notifications for one customer / restaurant / driver
    resource function get [string recipientId]() returns json|error {
        stream<NotificationRow, sql:Error?> rs = db->query(
            `SELECT * FROM notifications WHERE recipient_id = ${recipientId} ORDER BY id`);
        NotificationRow[] rows = check from NotificationRow r in rs
            select r;
        return rows.toJson();
    }
}

listener kafka:Listener notificationConsumer = new (kafkaBroker, {
    groupId: "notification-service",
    topics: ["orders.created", "restaurant.rejected", "payments.completed", "payments.failed",
             "orders.confirmed", "orders.preparing", "orders.ready", "delivery.assigned",
             "delivery.pickedup", "delivery.completed", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on notificationConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}