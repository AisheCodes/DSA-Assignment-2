import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerinax/kafka;

// Namibia is UTC+2. Used to check opening hours.
configurable int utcOffsetHours = 2;

type RestaurantInput record {|
    string name;
    string address;
    string opens_at = "00:00";
    string closes_at = "23:59";
|};

type RestaurantRow record {|
    string restaurant_id;
    string name;
    string address;
    string opens_at;
    string closes_at;
|};

type MenuInput record {|
    string name;
    decimal price;
    int stock = 0;
|};

type MenuRow record {|
    string item_id;
    string restaurant_id;
    string name;
    decimal price;
    int stock;
|};

type OrderedItem record {
    string item_id;
    int quantity;
};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS restaurants (
        restaurant_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        address TEXT NOT NULL,
        opens_at TEXT NOT NULL,
        closes_at TEXT NOT NULL)`);
    check exec(`CREATE TABLE IF NOT EXISTS menu_items (
        item_id TEXT PRIMARY KEY,
        restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id),
        name TEXT NOT NULL,
        price NUMERIC(10,2) NOT NULL,
        stock INT NOT NULL DEFAULT 0)`);
    check exec(`CREATE TABLE IF NOT EXISTS reservations (
        order_id TEXT NOT NULL,
        item_id TEXT NOT NULL,
        quantity INT NOT NULL,
        released BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (order_id, item_id))`);
}

function toMinutes(string hhmm) returns int|error {
    int h = check int:fromString(hhmm.substring(0, 2));
    int m = check int:fromString(hhmm.substring(3, 5));
    return h * 60 + m;
}

function isOpen(RestaurantRow r) returns boolean|error {
    time:Civil c = time:utcToCivil(time:utcNow());
    int cur = ((c.hour + utcOffsetHours) % 24) * 60 + c.minute;
    int opens = check toMinutes(r.opens_at);
    int closes = check toMinutes(r.closes_at);
    if opens <= closes {
        return cur >= opens && cur <= closes;
    }
    return cur >= opens || cur <= closes;
}

// Puts reserved stock back (only for reservations not yet released)
function releaseStock(string orderId) returns error? {
    check exec(`UPDATE menu_items m SET stock = m.stock + r.quantity
        FROM reservations r
        WHERE r.order_id = ${orderId} AND r.released = FALSE AND m.item_id = r.item_id`);
    check exec(`UPDATE reservations SET released = TRUE WHERE order_id = ${orderId}`);
}

// Reserves stock atomically per item. Returns the order total, or a rejection reason.
function reserve(string orderId, string restaurantId, OrderedItem[] items) returns decimal|string|error {
    decimal total = 0d;
    foreach OrderedItem it in items {
        if it.quantity <= 0 {
            check releaseStock(orderId);
            return "Invalid quantity for item " + it.item_id;
        }
        decimal|sql:Error price = db->queryRow(
            `SELECT price FROM menu_items WHERE item_id = ${it.item_id} AND restaurant_id = ${restaurantId}`);
        if price is sql:NoRowsError {
            check releaseStock(orderId);
            return "Unknown item " + it.item_id;
        }
        if price is sql:Error {
            return price;
        }
        sql:ExecutionResult r = check db->execute(
            `UPDATE menu_items SET stock = stock - ${it.quantity}
             WHERE item_id = ${it.item_id} AND restaurant_id = ${restaurantId} AND stock >= ${it.quantity}`);
        if r.affectedRowCount != 1 {
            check releaseStock(orderId);
            return "Out of stock: " + it.item_id;
        }
        check exec(`INSERT INTO reservations (order_id, item_id, quantity) VALUES (${orderId}, ${it.item_id}, ${it.quantity})
            ON CONFLICT (order_id, item_id) DO UPDATE SET quantity = reservations.quantity + EXCLUDED.quantity`);
        total += price * <decimal>it.quantity;
    }
    return total;
}

function handleEvent(Event ev) returns error? {
    match ev.'type {
        "ORDER_CREATED" => {
            OrderInfo o = check ev.payload.cloneWithType();
            json itemsJson = check ev.payload.items;
            OrderedItem[] items = check itemsJson.cloneWithType();

            string reason = "";
            decimal total = 0d;
            RestaurantRow|sql:Error rest = db->queryRow(
                `SELECT * FROM restaurants WHERE restaurant_id = ${o.restaurant_id}`);
            if rest is sql:NoRowsError {
                reason = "Unknown restaurant";
            } else if rest is sql:Error {
                return rest;
            } else {
                boolean open = check isOpen(rest);
                if !open {
                    reason = "Restaurant is closed";
                } else {
                    decimal|string res = check reserve(o.order_id, o.restaurant_id, items);
                    if res is string {
                        reason = res;
                    } else {
                        total = res;
                    }
                }
            }

            json payload = {
                order_id: o.order_id,
                customer_id: o.customer_id,
                restaurant_id: o.restaurant_id,
                total_amount: total,
                reason: reason
            };
            if reason == "" {
                check publish("restaurant.accepted", o.order_id, "RESTAURANT_ACCEPTED", payload);
            } else {
                log:printWarn("Order " + o.order_id + " rejected: " + reason);
                check publish("restaurant.rejected", o.order_id, "RESTAURANT_REJECTED", payload);
            }
        }
        "ORDER_CANCELLED" => {
            check releaseStock(ev.orderId);
        }
    }
}

listener http:Listener api = new (httpPort);

service /restaurants on api {

    resource function post .(RestaurantInput req) returns json|error {
        string id = newId();
        check exec(`INSERT INTO restaurants (restaurant_id, name, address, opens_at, closes_at)
            VALUES (${id}, ${req.name}, ${req.address}, ${req.opens_at}, ${req.closes_at})`);
        json res = {
            restaurant_id: id,
            name: req.name,
            address: req.address,
            opens_at: req.opens_at,
            closes_at: req.closes_at
        };
        return res;
    }

    resource function get .() returns json|error {
        stream<RestaurantRow, sql:Error?> rs = db->query(`SELECT * FROM restaurants ORDER BY name`);
        RestaurantRow[] rows = check from RestaurantRow r in rs
            select r;
        return rows.toJson();
    }

    resource function get [string rid]() returns json|http:NotFound|error {
        RestaurantRow|sql:Error r = db->queryRow(`SELECT * FROM restaurants WHERE restaurant_id = ${rid}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is sql:Error {
            return r;
        }
        return r.toJson();
    }

    resource function put [string rid]/hours(string opens_at, string closes_at) returns json|http:NotFound|error {
        sql:ExecutionResult r = check db->execute(
            `UPDATE restaurants SET opens_at = ${opens_at}, closes_at = ${closes_at} WHERE restaurant_id = ${rid}`);
        if r.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }
        json res = {restaurant_id: rid, opens_at: opens_at, closes_at: closes_at};
        return res;
    }

    resource function post [string rid]/menu(MenuInput req) returns json|error {
        string id = newId();
        check exec(`INSERT INTO menu_items (item_id, restaurant_id, name, price, stock)
            VALUES (${id}, ${rid}, ${req.name}, ${req.price}, ${req.stock})`);
        json res = {item_id: id, restaurant_id: rid, name: req.name, price: req.price, stock: req.stock};
        return res;
    }

    resource function get [string rid]/menu() returns json|error {
        stream<MenuRow, sql:Error?> rs = db->query(
            `SELECT * FROM menu_items WHERE restaurant_id = ${rid} ORDER BY name`);
        MenuRow[] rows = check from MenuRow r in rs
            select r;
        return rows.toJson();
    }

    // real-time inventory update
    resource function put [string rid]/menu/[string itemId]/stock(int quantity) returns json|http:NotFound|error {
        sql:ExecutionResult r = check db->execute(
            `UPDATE menu_items SET stock = ${quantity} WHERE item_id = ${itemId} AND restaurant_id = ${rid}`);
        if r.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }
        json res = {item_id: itemId, stock: quantity};
        return res;
    }

    // kitchen actions: the Order service validates and applies the state change
    resource function post [string rid]/orders/[string orderId]/preparing() returns json|error {
        json p = {order_id: orderId, restaurant_id: rid};
        check publish("kitchen.preparing", orderId, "KITCHEN_PREPARING", p);
        return p;
    }

    resource function post [string rid]/orders/[string orderId]/ready() returns json|error {
        json p = {order_id: orderId, restaurant_id: rid};
        check publish("kitchen.ready", orderId, "KITCHEN_READY", p);
        return p;
    }
}

listener kafka:Listener restaurantConsumer = new (kafkaBroker, {
    groupId: "restaurant-service",
    topics: ["orders.created", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on restaurantConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}