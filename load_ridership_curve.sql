Drop table if exists ridership_curve;

CREATE TABLE ridership_curve (
  hour INTEGER PRIMARY KEY,
  ridership_ratio float NOT NULL
);

COPY ridership_curve(
  hour,
  ridership_ratio
) FROM '/temp/data/optimising_public_transportation/ridership_curve.csv' DELIMITER ',' CSV HEADER;