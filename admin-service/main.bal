import ballerina/http;
import ballerina/sql;
import ballerinax/kafka;

type DeliveryInfo record {
    string order_id;
    string driver_id;
};

type RestaurantReport record {|
    string restaurant_id;
    int total_orders;
    int delivered;
    int cancelled;
    decimal revenue;
|};

type DeliverySummary record {|
    int total_assigned;
    int completed;
    decimal avg_minutes;
|};

type DriverReport record {|
    string driver_id;
    int deliveries;
|};

function init() returns error? {
    check initCommon();
    check exec(`CREATE TABLE IF NOT EXISTS order_facts (
        order_id TEXT PRIMARY KEY,
        restaurant_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        status TEXT NOT NULL,
        status_rank INT NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
        updated_epoch BIGINT NOT NULL)`);
    check exec(`CREATE TABLE IF NOT EXISTS delivery_facts (
        order_id TEXT PRIMARY KEY,
        driver_id TEXT NOT NULL,
        assigned_epoch BIGINT NOT NULL,
        completed_epoch BIGINT)`);
}

function handleEvent(Event ev) returns error? {
    match ev.'type {
        "DRIVER_ASSIGNED" => {
            DeliveryInfo d = check ev.payload.cloneWithType();
            check exec(`INSERT INTO delivery_facts (order_id, driver_id, assigned_epoch)
                VALUES (${d.order_id}, ${d.driver_id}, ${nowEpoch()}) ON CONFLICT (order_id) DO NOTHING`);
        }
        "DELIVERY_COMPLETED" => {
            DeliveryInfo d = check ev.payload.cloneWithType();
            check exec(`UPDATE delivery_facts SET completed_epoch = ${nowEpoch()} WHERE order_id = ${d.order_id}`);
        }
        _ => {
            OrderInfo o = check ev.payload.cloneWithType();
            check exec(`INSERT INTO order_facts (order_id, restaurant_id, customer_id, status, status_rank, total_amount, updated_epoch)
                VALUES (${o.order_id}, ${o.restaurant_id}, ${o.customer_id}, ${o.status}, ${statusRank(o.status)}, ${o.total_amount}, ${nowEpoch()})
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    status_rank = EXCLUDED.status_rank,
                    total_amount = EXCLUDED.total_amount,
                    updated_epoch = EXCLUDED.updated_epoch
                WHERE order_facts.status_rank <= EXCLUDED.status_rank`);
        }
    }
}

listener http:Listener api = new (httpPort);

service /reports on api {

    // orders, cancellations and revenue per restaurant
    resource function get restaurants() returns json|error {
        stream<RestaurantReport, sql:Error?> rs = db->query(
            `SELECT restaurant_id,
                    COUNT(*) AS total_orders,
                    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered,
                    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled,
                    COALESCE(SUM(total_amount) FILTER (WHERE status = 'DELIVERED'), 0) AS revenue
             FROM order_facts GROUP BY restaurant_id ORDER BY total_orders DESC`);
        RestaurantReport[] rows = check from RestaurantReport r in rs
            select r;
        return rows.toJson();
    }

    // delivery performance: totals, average delivery time, deliveries per driver
    resource function get deliveries() returns json|error {
        DeliverySummary s = check db->queryRow(
            `SELECT COUNT(*) AS total_assigned,
                    COUNT(*) FILTER (WHERE completed_epoch IS NOT NULL) AS completed,
                    COALESCE(AVG(completed_epoch - assigned_epoch) FILTER (WHERE completed_epoch IS NOT NULL) / 60.0, 0) AS avg_minutes
             FROM delivery_facts`);
        stream<DriverReport, sql:Error?> rs = db->query(
            `SELECT driver_id, COUNT(*) AS deliveries
             FROM delivery_facts WHERE completed_epoch IS NOT NULL
             GROUP BY driver_id ORDER BY deliveries DESC`);
        DriverReport[] drivers = check from DriverReport d in rs
            select d;
        json res = {summary: s.toJson(), per_driver: drivers.toJson()};
        return res;
    }
}

listener kafka:Listener adminConsumer = new (kafkaBroker, {
    groupId: "admin-service",
    topics: ["orders.created", "orders.confirmed", "orders.preparing", "orders.ready",
             "orders.out_for_delivery", "orders.delivered", "orders.cancelled",
             "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on adminConsumer {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        dispatch(records, handleEvent);
    }
}