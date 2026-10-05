import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable int httpPort = 8081;
configurable string kafkaBroker = "localhost:9092";
configurable string dbHost = "localhost";
configurable int dbPort = 5432;
configurable string dbUser = "postgres";
configurable string dbPassword = "postgres";
configurable string dbName = "customers_db";

final postgresql:Client db = check new (dbHost, dbUser, dbPassword, dbName, dbPort);
final kafka:Producer producer = check new (kafkaBroker);

// Every Kafka message uses this envelope. The message key is always the orderId,
// so all events of one order go to the same partition and stay in order.
type Event record {
    string eventId;
    string orderId;
    string 'type;
    string timestamp;
    json payload;
};

// Snapshot of an order carried in most event payloads
type OrderInfo record {
    string order_id;
    string customer_id;
    string restaurant_id;
    decimal total_amount = 0d;
    string status = "";
};

function newId() returns string => uuid:createType1AsString();

function nowStr() returns string => time:utcToString(time:utcNow());

function nowEpoch() returns int => time:utcNow()[0];

function exec(sql:ParameterizedQuery q) returns error? {
    sql:ExecutionResult _ = check db->execute(q);
}

function initCommon() returns error? {
    check exec(`CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY)`);
}

// Higher rank = later in the order lifecycle (protects against out-of-order events across topics)
function statusRank(string s) returns int {
    match s {
        "CREATED" => { return 1; }
        "CONFIRMED" => { return 2; }
        "PREPARING" => { return 3; }
        "READY" => { return 4; }
        "OUT_FOR_DELIVERY" => { return 5; }
        "DELIVERED" => { return 6; }
        "CANCELLED" => { return 7; }
    }
    return 0;
}

function publish(string topic, string orderId, string eventType, json payload) returns error? {
    Event ev = {
        eventId: newId(),
        orderId: orderId,
        'type: eventType,
        timestamp: nowStr(),
        payload: payload
    };
    check producer->send({
        topic: topic,
        key: orderId.toBytes(),
        value: ev.toJsonString().toBytes()
    });
    log:printInfo("PUBLISHED " + eventType + " to " + topic + " order=" + orderId);
}

function parseEvent(kafka:BytesConsumerRecord rec) returns Event|error {
    string text = check string:fromBytes(rec.value);
    json j = check text.fromJsonString();
    return j.cloneWithType(Event);
}

// true only the first time an eventId is seen (idempotent consumer)
function isFirstTime(string eventId) returns boolean|error {
    sql:ExecutionResult r = check db->execute(
        `INSERT INTO processed_events (event_id) VALUES (${eventId}) ON CONFLICT DO NOTHING`);
    return r.affectedRowCount == 1;
}

function dispatch(kafka:BytesConsumerRecord[] records, function (Event) returns error? handler) {
    foreach kafka:BytesConsumerRecord rec in records {
        Event|error ev = parseEvent(rec);
        if ev is error {
            log:printError("Could not parse event", 'error = ev);
            continue;
        }
        boolean|error first = isFirstTime(ev.eventId);
        if first is error {
            log:printError("Idempotency check failed", 'error = first);
            continue;
        }
        if !first {
            continue;
        }
        error? result = handler(ev);
        if result is error {
            log:printError("Handler failed for " + ev.'type, 'error = result);
        }
    }
}