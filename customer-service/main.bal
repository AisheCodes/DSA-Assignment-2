import ballerina/http;
import ballerina/sql;
import ballerinax/kafka;

type CustomerInput record {|
    string name;
    string email = "";
    string phone = "";
|};

type CustomerRow record {|
    string customer_id;
    string name;
    string email;
    string phone;
|};

type AddressInput record {|
    string label = "Home";
    string street;
    string city;
|};

type AddressRow record {|
    string address_id;
    string customer_id;
    string label;
    string street;
    string city;
|};

type CustomerOrderRow record {|
    string order_id;
    string customer_id;
    string restaurant_id;
    string status;
    decimal total_amount;
    string updated_at;
|};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT NOT NULL)`);
    check exec(`CREATE TABLE IF NOT EXISTS addresses (
        address_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL REFERENCES customers(customer_id),
        label TEXT NOT NULL,
        street TEXT NOT NULL,
        city TEXT NOT NULL)`);
    // order history is built purely from Kafka order events
    check exec(`CREATE TABLE IF NOT EXISTS customer_orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        restaurant_id TEXT NOT NULL,
        status TEXT NOT NULL,
        status_rank INT NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL)`);
}

function handleEvent(Event ev) returns error? {
    OrderInfo o = check ev.payload.cloneWithType();
    check exec(`INSERT INTO customer_orders (order_id, customer_id, restaurant_id, status, status_rank, total_amount, updated_at)
        VALUES (${o.order_id}, ${o.customer_id}, ${o.restaurant_id}, ${o.status}, ${statusRank(o.status)}, ${o.total_amount}, ${nowStr()})
        ON CONFLICT (order_id) DO UPDATE SET
            status = EXCLUDED.status,
            status_rank = EXCLUDED.status_rank,
            total_amount = EXCLUDED.total_amount,
            updated_at = EXCLUDED.updated_at
        WHERE customer_orders.status_rank <= EXCLUDED.status_rank`);
}

listener http:Listener api = new (httpPort);

service /customers on api {

    resource function post .(CustomerInput req) returns json|error {
        string id = newId();
        check exec(`INSERT INTO customers (customer_id, name, email, phone)
            VALUES (${id}, ${req.name}, ${req.email}, ${req.phone})`);
        json res = {customer_id: id, name: req.name, email: req.email, phone: req.phone};
        return res;
    }

    resource function get .() returns json|error {
        stream<CustomerRow, sql:Error?> rs = db->query(`SELECT * FROM customers ORDER BY name`);
        CustomerRow[] rows = check from CustomerRow r in rs
            select r;
        return rows.toJson();
    }

    resource function get [string customerId]() returns json|http:NotFound|error {
        CustomerRow|sql:Error r = db->queryRow(`SELECT * FROM customers WHERE customer_id = ${customerId}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is sql:Error {
            return r;
        }
        return r.toJson();
    }

    resource function post [string customerId]/addresses(AddressInput req) returns json|error {
        string id = newId();
        check exec(`INSERT INTO addresses (address_id, customer_id, label, street, city)
            VALUES (${id}, ${customerId}, ${req.label}, ${req.street}, ${req.city})`);
        json res = {address_id: id, customer_id: customerId, label: req.label, street: req.street, city: req.city};
        return res;
    }

    resource function get [string customerId]/addresses() returns json|error {
        stream<AddressRow, sql:Error?> rs = db->query(`SELECT * FROM addresses WHERE customer_id = ${customerId}`);
        AddressRow[] rows = check from AddressRow r in rs
            select r;
        return rows.toJson();
    }

    resource function get [string customerId]/orders() returns json|error {
        stream<CustomerOrderRow, sql:Error?> rs = db->query(
            `SELECT order_id, customer_id, restaurant_id, status, total_amount, updated_at
             FROM customer_orders WHERE customer_id = ${customerId} ORDER BY updated_at DESC`);
        CustomerOrderRow[] rows = check from CustomerOrderRow r in rs
            select r;
        return rows.toJson();
    }
}

listener kafka:Listener customerConsumer = new (kafkaBroker, {
    groupId: "customer-service",
    topics: ["orders.created", "orders.confirmed", "orders.preparing", "orders.ready",
             "orders.out_for_delivery", "orders.delivered", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on customerConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}