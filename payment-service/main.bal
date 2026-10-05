import ballerina/http;
import ballerina/log;
import ballerina/random;
import ballerina/sql;
import ballerinax/kafka;

// Percentage of payments that succeed (simulation). Set to 100 to always succeed.
configurable int successRate = 90;

type PaymentRow record {|
    string payment_id;
    string order_id;
    string customer_id;
    string restaurant_id;
    decimal amount;
    string status;
    string created_at;
|};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS payments (
        payment_id TEXT PRIMARY KEY,
        order_id TEXT UNIQUE NOT NULL,
        customer_id TEXT NOT NULL,
        restaurant_id TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL)`);
}

function handleEvent(Event ev) returns error? {
    match ev.'type {
        "RESTAURANT_ACCEPTED" => {
            OrderInfo o = check ev.payload.cloneWithType();
            int roll = check random:createIntInRange(0, 100);
            boolean ok = roll < successRate;
            string status = ok ? "COMPLETED" : "FAILED";
            string payId = newId();
            check exec(`INSERT INTO payments (payment_id, order_id, customer_id, restaurant_id, amount, status, created_at)
                VALUES (${payId}, ${o.order_id}, ${o.customer_id}, ${o.restaurant_id}, ${o.total_amount}, ${status}, ${nowStr()})
                ON CONFLICT (order_id) DO NOTHING`);
            json payload = {
                payment_id: payId,
                order_id: o.order_id,
                customer_id: o.customer_id,
                restaurant_id: o.restaurant_id,
                amount: o.total_amount,
                status: status
            };
            log:printInfo("Payment " + status + " for order " + o.order_id);
            if ok {
                check publish("payments.completed", o.order_id, "PAYMENT_COMPLETED", payload);
            } else {
                check publish("payments.failed", o.order_id, "PAYMENT_FAILED", payload);
            }
        }
        "ORDER_CANCELLED" => {
            // refund completed payments of cancelled orders
            check exec(`UPDATE payments SET status = 'REFUNDED' WHERE order_id = ${ev.orderId} AND status = 'COMPLETED'`);
        }
    }
}

listener http:Listener api = new (httpPort);

service /payments on api {

    resource function get .() returns json|error {
        stream<PaymentRow, sql:Error?> rs = db->query(`SELECT * FROM payments ORDER BY created_at DESC LIMIT 100`);
        PaymentRow[] rows = check from PaymentRow r in rs
            select r;
        return rows.toJson();
    }

    resource function get [string orderId]() returns json|http:NotFound|error {
        PaymentRow|sql:Error r = db->queryRow(`SELECT * FROM payments WHERE order_id = ${orderId}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is sql:Error {
            return r;
        }
        return r.toJson();
    }
}

listener kafka:Listener paymentConsumer = new (kafkaBroker, {
    groupId: "payment-service",
    topics: ["restaurant.accepted", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on paymentConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}