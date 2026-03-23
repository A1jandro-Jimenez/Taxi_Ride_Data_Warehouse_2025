CREATE OR REPLACE MATERIALIZED VIEW taxirides2025.gold.trips_gold
AS(
  SELECT
  t.TripID,
  t.hour_of_day,
  t.trip_duration_minutes,
  t.passenger_count,
  t.trip_distance_miles,
  t.store_and_fwd_flag,
  p.payment_type,
  t.tip_amount,
  t.total_amount,
  cal.month,
  cal.day_of_month, 
  cal.day_of_week_abbr,
  cal.is_weekend,
  cal.is_weekday,
  r.Rate,
  zpu.Zone AS pickup_zone,
  zpu.Borough AS pickup_borough,
  dof.Zone AS dropoff_zone,
  dof.Borough AS dropoff_borough
  FROM taxirides2025.silver.trips t
  JOIN taxirides2025.silver.calendar cal
    ON t.DateID = cal.date_id
  JOIN taxirides2025.silver.ratecode r 
    ON t.RatecodeID = r.RateCodeID
  JOIN taxirides2025.silver.payment_type p 
    ON t.payment_type = p.PaymentTypeID
  LEFT JOIN taxirides2025.silver.zone zpu
    ON t.PULocationID = zpu.LocationID
  LEFT JOIN taxirides2025.silver.zone dof
    ON t.DOLocationID = dof.LocationID
)