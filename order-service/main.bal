import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

type OrderItem record {|
    string item_id;
    int quantity;
|};

type OrderRequest record {|
    string customer_id;
    string restaurant_id;
    OrderItem[] items;
|};

type OrderRow record {|
    string order_id;
    string customer_id;
    string restaurant_id;
    string items;
    decimal total_amount;
    string status;
    string created_at;
    string updated_at;
|};

type HistoryRow record {|
    int id;
    string order_id;
    string status;
    string changed_at;
|};

// The order state machine: allowed transitions
final map<string[]> transitions = {
    "CREATED": ["CONFIRMED", "CANCELLED"],
    "CONFIRMED": ["PREPARING", "CANCELLED"],
    "PREPARING": ["READY", "CANCELLED"],
    "READY": ["OUT_FOR_DELIVERY"],
    "OUT_FOR_DELIVERY": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": []
};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        restaurant_id TEXT NOT NULL,
        items TEXT NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL)`);
    check exec(`CREATE TABLE IF NOT EXISTS order_history (
        id SERIAL PRIMARY KEY,
        order_id TEXT NOT NULL,
        status TEXT NOT NULL,
        changed_at TEXT NOT NULL)`);
}

// Validates the transition, saves it, records history and publishes the event.
// Returns false when the order does not exist or the transition is not allowed.
function changeStatus(string orderId, string target, string eventType, string topic) returns boolean|error {
    OrderRow|sql:Error r = db->queryRow(`SELECT * FROM orders WHERE order_id = ${orderId}`);
    if r is sql:NoRowsError {
        return false;
    }
    if r is sql:Error {
        return r;
    }
    string[] allowed = transitions[r.status] ?: [];
    if allowed.indexOf(target) is () {
        return false;
    }
    string ts = nowStr();
    check exec(`UPDATE orders SET status = ${target}, updated_at = ${ts} WHERE order_id = ${orderId}`);
    check exec(`INSERT INTO order_history (order_id, status, changed_at) VALUES (${orderId}, ${target}, ${ts})`);
    r.status = target;
    check publish(topic, orderId, eventType, r.toJson());
    return true;
}

function move(string orderId, string target, string eventType, string topic) returns error? {
    boolean moved = check changeStatus(orderId, target, eventType, topic);
    if !moved {
        log:printWarn("Ignored invalid transition to " + target + " for order " + orderId);
    }
}

listener http:Listener api = new (httpPort);

service /orders on api {

    resource function post .(OrderRequest req) returns json|http:BadRequest|error {
        if req.items.length() == 0 {
            return http:BAD_REQUEST;
        }
        string id = newId();
        string ts = nowStr();
        check exec(`INSERT INTO orders (order_id, customer_id, restaurant_id, items, status, created_at, updated_at)
            VALUES (${id}, ${req.customer_id}, ${req.restaurant_id}, ${req.items.toJsonString()}, 'CREATED', ${ts}, ${ts})`);
        check exec(`INSERT INTO order_history (order_id, status, changed_at) VALUES (${id}, 'CREATED', ${ts})`);
        json payload = {
            order_id: id,
            customer_id: req.customer_id,
            restaurant_id: req.restaurant_id,
            status: "CREATED",
            total_amount: 0,
            items: req.items.toJson()
        };
        check publish("orders.created", id, "ORDER_CREATED", payload);
        return payload;
    }

    resource function get .() returns json|error {
        stream<OrderRow, sql:Error?> rs = db->query(`SELECT * FROM orders ORDER BY created_at DESC LIMIT 100`);
        OrderRow[] rows = check from OrderRow r in rs
            select r;
        return rows.toJson();
    }

    resource function get [string orderId]() returns json|http:NotFound|error {
        OrderRow|sql:Error r = db->queryRow(`SELECT * FROM orders WHERE order_id = ${orderId}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is sql:Error {
            return r;
        }
        return r.toJson();
    }

    resource function get [string orderId]/history() returns json|error {
        stream<HistoryRow, sql:Error?> rs = db->query(
            `SELECT * FROM order_history WHERE order_id = ${orderId} ORDER BY id`);
        HistoryRow[] rows = check from HistoryRow r in rs
            select r;
        return rows.toJson();
    }

    resource function put [string orderId]/cancel() returns json|http:Conflict|error {
        boolean ok = check changeStatus(orderId, "CANCELLED", "ORDER_CANCELLED", "orders.cancelled");
        if !ok {
            return http:CONFLICT;
        }
        json res = {order_id: orderId, status: "CANCELLED"};
        return res;
    }
}

function handleEvent(Event ev) returns error? {
    string id = ev.orderId;
    match ev.'type {
        "RESTAURANT_ACCEPTED" => {
            OrderInfo info = check ev.payload.cloneWithType();
            check exec(`UPDATE orders SET total_amount = ${info.total_amount} WHERE order_id = ${id}`);
        }
        "RESTAURANT_REJECTED"|"PAYMENT_FAILED" => {
            check move(id, "CANCELLED", "ORDER_CANCELLED", "orders.cancelled");
        }
        "PAYMENT_COMPLETED" => {
            check move(id, "CONFIRMED", "ORDER_CONFIRMED", "orders.confirmed");
        }
        "KITCHEN_PREPARING" => {
            check move(id, "PREPARING", "ORDER_PREPARING", "orders.preparing");
        }
        "KITCHEN_READY" => {
            check move(id, "READY", "ORDER_READY", "orders.ready");
        }
        "DELIVERY_PICKED_UP" => {
            check move(id, "OUT_FOR_DELIVERY", "ORDER_OUT_FOR_DELIVERY", "orders.out_for_delivery");
        }
        "DELIVERY_COMPLETED" => {
            check move(id, "DELIVERED", "ORDER_DELIVERED", "orders.delivered");
        }
    }
}

listener kafka:Listener orderConsumer = new (kafkaBroker, {
    groupId: "order-service",
    topics: ["restaurant.accepted", "restaurant.rejected", "payments.completed", "payments.failed",
             "kitchen.preparing", "kitchen.ready", "delivery.pickedup", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on orderConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}