import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

type DriverInput record {|
    string name;
    string phone = "";
    string vehicle = "Motorbike";
|};

type DriverRow record {|
    string driver_id;
    string name;
    string phone;
    string vehicle;
    boolean available;
    float? lat;
    float? lng;
|};

type DeliveryRow record {|
    string order_id;
    string customer_id;
    string restaurant_id;
    string? driver_id;
    string status;
    string created_at;
    string updated_at;
|};

type TrackingRow record {|
    string order_id;
    string status;
    string? driver_id;
    float? lat;
    float? lng;
|};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS drivers (
        driver_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        phone TEXT NOT NULL,
        vehicle TEXT NOT NULL,
        available BOOLEAN NOT NULL DEFAULT TRUE,
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION)`);
    check exec(`CREATE TABLE IF NOT EXISTS deliveries (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        restaurant_id TEXT NOT NULL,
        driver_id TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL)`);
}

// Assigns waiting deliveries (oldest first) to available drivers.
function tryAssignPending() returns error? {
    while true {
        DeliveryRow|sql:Error p = db->queryRow(
            `SELECT * FROM deliveries WHERE status = 'PENDING' ORDER BY created_at LIMIT 1`);
        if p is sql:NoRowsError {
            return;
        }
        if p is sql:Error {
            return p;
        }
        string|sql:Error d = db->queryRow(
            `SELECT driver_id FROM drivers WHERE available = TRUE ORDER BY driver_id LIMIT 1`);
        if d is sql:NoRowsError {
            return;
        }
        if d is sql:Error {
            return d;
        }
        // claim the driver; if someone else took them meanwhile, try again
        sql:ExecutionResult claim = check db->execute(
            `UPDATE drivers SET available = FALSE WHERE driver_id = ${d} AND available = TRUE`);
        if claim.affectedRowCount != 1 {
            continue;
        }
        check exec(`UPDATE deliveries SET driver_id = ${d}, status = 'ASSIGNED', updated_at = ${nowStr()}
            WHERE order_id = ${p.order_id}`);
        json payload = {
            order_id: p.order_id,
            customer_id: p.customer_id,
            restaurant_id: p.restaurant_id,
            driver_id: d
        };
        log:printInfo("Driver " + d + " assigned to order " + p.order_id);
        check publish("delivery.assigned", p.order_id, "DRIVER_ASSIGNED", payload);
    }
}

function handleEvent(Event ev) returns error? {
    if ev.'type == "ORDER_READY" {
        OrderInfo o = check ev.payload.cloneWithType();
        string ts = nowStr();
        check exec(`INSERT INTO deliveries (order_id, customer_id, restaurant_id, status, created_at, updated_at)
            VALUES (${o.order_id}, ${o.customer_id}, ${o.restaurant_id}, 'PENDING', ${ts}, ${ts})
            ON CONFLICT (order_id) DO NOTHING`);
        check tryAssignPending();
    }
}

listener http:Listener api = new (httpPort);

service /drivers on api {

    resource function post .(DriverInput req) returns json|error {
        string id = newId();
        check exec(`INSERT INTO drivers (driver_id, name, phone, vehicle, available)
            VALUES (${id}, ${req.name}, ${req.phone}, ${req.vehicle}, TRUE)`);
        check tryAssignPending();
        json res = {driver_id: id, name: req.name, phone: req.phone, vehicle: req.vehicle, available: true};
        return res;
    }

    resource function get .() returns json|error {
        stream<DriverRow, sql:Error?> rs = db->query(`SELECT * FROM drivers ORDER BY name`);
        DriverRow[] rows = check from DriverRow r in rs
            select r;
        return rows.toJson();
    }

    resource function put [string driverId]/availability(boolean available) returns json|http:NotFound|error {
        sql:ExecutionResult r = check db->execute(
            `UPDATE drivers SET available = ${available} WHERE driver_id = ${driverId}`);
        if r.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }
        if available {
            check tryAssignPending();
        }
        json res = {driver_id: driverId, available: available};
        return res;
    }

    // driver location updates (used for tracking)
    resource function put [string driverId]/location(float lat, float lng) returns json|http:NotFound|error {
        sql:ExecutionResult r = check db->execute(
            `UPDATE drivers SET lat = ${lat}, lng = ${lng} WHERE driver_id = ${driverId}`);
        if r.affectedRowCount == 0 {
            return http:NOT_FOUND;
        }
        json res = {driver_id: driverId, lat: lat, lng: lng};
        return res;
    }
}

service /deliveries on api {

    resource function get .() returns json|error {
        stream<DeliveryRow, sql:Error?> rs = db->query(`SELECT * FROM deliveries ORDER BY created_at DESC LIMIT 100`);
        DeliveryRow[] rows = check from DeliveryRow r in rs
            select r;
        return rows.toJson();
    }

    // status + current driver location
    resource function get [string orderId]() returns json|http:NotFound|error {
        TrackingRow|sql:Error r = db->queryRow(
            `SELECT d.order_id, d.status, d.driver_id, dr.lat, dr.lng
             FROM deliveries d LEFT JOIN drivers dr ON dr.driver_id = d.driver_id
             WHERE d.order_id = ${orderId}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is sql:Error {
            return r;
        }
        return r.toJson();
    }

    resource function post [string orderId]/pickup() returns json|http:NotFound|http:Conflict|error {
        DeliveryRow|sql:Error d = db->queryRow(`SELECT * FROM deliveries WHERE order_id = ${orderId}`);
        if d is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if d is sql:Error {
            return d;
        }
        if d.status != "ASSIGNED" {
            return http:CONFLICT;
        }
        check exec(`UPDATE deliveries SET status = 'PICKED_UP', updated_at = ${nowStr()} WHERE order_id = ${orderId}`);
        json p = {
            order_id: orderId,
            customer_id: d.customer_id,
            restaurant_id: d.restaurant_id,
            driver_id: d.driver_id
        };
        check publish("delivery.pickedup", orderId, "DELIVERY_PICKED_UP", p);
        return p;
    }

    resource function post [string orderId]/complete() returns json|http:NotFound|http:Conflict|error {
        DeliveryRow|sql:Error d = db->queryRow(`SELECT * FROM deliveries WHERE order_id = ${orderId}`);
        if d is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if d is sql:Error {
            return d;
        }
        if d.status != "PICKED_UP" {
            return http:CONFLICT;
        }
        check exec(`UPDATE deliveries SET status = 'DELIVERED', updated_at = ${nowStr()} WHERE order_id = ${orderId}`);
        string? did = d.driver_id;
        if did is string {
            check exec(`UPDATE drivers SET available = TRUE WHERE driver_id = ${did}`);
        }
        json p = {
            order_id: orderId,
            customer_id: d.customer_id,
            restaurant_id: d.restaurant_id,
            driver_id: d.driver_id
        };
        check publish("delivery.completed", orderId, "DELIVERY_COMPLETED", p);
        check tryAssignPending();
        return p;
    }
}

listener kafka:Listener deliveryConsumer = new (kafkaBroker, {
    groupId: "delivery-service",
    topics: ["orders.ready"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on deliveryConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}