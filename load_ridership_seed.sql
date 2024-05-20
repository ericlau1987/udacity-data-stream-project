Drop table if exists ridership_seed;

CREATE TABLE ridership_seed (
  station_id INTEGER PRIMARY KEY,
  stationame VARCHAR NOT NULL,
  month_beginning date,
  avg_weekday_rides decimal(19,6) NOT NULL,
  avg_saturday_rides decimal(19,6) NOT NULL,
  avg_sunday_holiday_rides decimal(19,6) NOT NULL,
  monthtotal INTEGER
);

COPY ridership_seed(
  station_id,
  stationame,
  month_beginning,
  avg_weekday_rides,
  avg_saturday_rides,
  avg_sunday_holiday_rides,
  monthtotal
) FROM '/temp/data/optimising_public_transportation/ridership_seed.csv' DELIMITER ',' CSV HEADER;