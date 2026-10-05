# test-flow.ps1 - walks one order through the whole platform
$ErrorActionPreference = "Stop"

function Api($method, $url, $body) {
    if ($body) {
        Invoke-RestMethod -Method $method -Uri $url -ContentType "application/json" -Body ($body | ConvertTo-Json -Depth 6)
    } else {
        Invoke-RestMethod -Method $method -Uri $url
    }
}
function Step($t) { Write-Host "`n== $t ==" -ForegroundColor Cyan }

function WaitOrder($orderId, $want) {
    for ($i = 0; $i -lt 30; $i++) {
        $o = Api Get "http://localhost:18080/orders/$orderId"
        if ($o.status -eq $want -or $o.status -eq "CANCELLED") { return $o }
        Start-Sleep -Seconds 1
    }
    return $o
}
function WaitDelivery($orderId, $want) {
    for ($i = 0; $i -lt 30; $i++) {
        try { $d = Api Get "http://localhost:18084/deliveries/$orderId"; if ($d.status -eq $want) { return $d } } catch {}
        Start-Sleep -Seconds 1
    }
    return $null
}

Step "1. Create customer, address, restaurant, menu item, driver"
$c = Api Post "http://localhost:18081/customers" @{ name = "Alice"; email = "alice@example.com"; phone = "0811234567" }
Api Post "http://localhost:18081/customers/$($c.customer_id)/addresses" @{ label = "Home"; street = "1 Independence Ave"; city = "Windhoek" } | Out-Null
$r = Api Post "http://localhost:18082/restaurants" @{ name = "Burger Palace"; address = "Windhoek" }
$m = Api Post "http://localhost:18082/restaurants/$($r.restaurant_id)/menu" @{ name = "Cheeseburger"; price = 45.50; stock = 20 }
$d = Api Post "http://localhost:18084/drivers" @{ name = "Bob"; phone = "0817654321"; vehicle = "Motorbike" }
Write-Host "customer=$($c.customer_id)"
Write-Host "restaurant=$($r.restaurant_id)  item=$($m.item_id)  driver=$($d.driver_id)"

Step "2. Place an order (2 x Cheeseburger)"
$o = Api Post "http://localhost:18080/orders" @{
    customer_id = $c.customer_id; restaurant_id = $r.restaurant_id
    items = @(@{ item_id = $m.item_id; quantity = 2 })
}
$oid = $o.order_id
Write-Host "order=$oid"

Step "3. Restaurant check -> payment -> CONFIRMED"
$o = WaitOrder $oid "CONFIRMED"
Write-Host "status=$($o.status) total=$($o.total_amount)"
if ($o.status -eq "CANCELLED") {
    Write-Host "Order was cancelled (payment simulation fails 10% of the time). Run test-flow.ps1 again." -ForegroundColor Yellow
    exit 0
}

Step "4. Kitchen: PREPARING then READY"
Api Post "http://localhost:18082/restaurants/$($r.restaurant_id)/orders/$oid/preparing" | Out-Null
$o = WaitOrder $oid "PREPARING"; Write-Host "status=$($o.status)"
Api Post "http://localhost:18082/restaurants/$($r.restaurant_id)/orders/$oid/ready" | Out-Null
$o = WaitOrder $oid "READY"; Write-Host "status=$($o.status)"

Step "5. Driver assigned, picks up, delivers"
$del = WaitDelivery $oid "ASSIGNED"
Write-Host "delivery status=$($del.status) driver=$($del.driver_id)"
Api Put "http://localhost:18084/drivers/$($d.driver_id)/location?lat=-22.5609&lng=17.0658" | Out-Null
Api Post "http://localhost:18084/deliveries/$oid/pickup" | Out-Null
$o = WaitOrder $oid "OUT_FOR_DELIVERY"; Write-Host "status=$($o.status)"
Api Post "http://localhost:18084/deliveries/$oid/complete" | Out-Null
$o = WaitOrder $oid "DELIVERED"; Write-Host "status=$($o.status)"

Start-Sleep -Seconds 3
Step "6. Results"
Write-Host "-- Order history:";      (Api Get "http://localhost:18080/orders/$oid/history") | Format-Table id, status, changed_at
Write-Host "-- Payment:";            (Api Get "http://localhost:18083/payments/$oid") | Format-List
Write-Host "-- Menu stock (20 - 2 = 18 expected):"; (Api Get "http://localhost:18082/restaurants/$($r.restaurant_id)/menu") | Format-Table name, price, stock
Write-Host "-- Customer order history:"; (Api Get "http://localhost:18081/customers/$($c.customer_id)/orders") | Format-Table order_id, status, total_amount
Write-Host "-- Customer notifications:"; (Api Get "http://localhost:18085/notifications/$($c.customer_id)") | Format-Table channel, message
Write-Host "-- Admin: restaurant report:"; (Api Get "http://localhost:18086/reports/restaurants") | Format-Table
Write-Host "-- Admin: delivery report:";   (Api Get "http://localhost:18086/reports/deliveries") | ConvertTo-Json -Depth 5
Write-Host "`nDone. Inspect the events at http://localhost:18090 (Kafka UI)." -ForegroundColor Green