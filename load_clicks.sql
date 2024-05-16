Drop table if exists clicks;

CREATE TABLE clicks (
  id INTEGER PRIMARY KEY,
  email text NOT NULL,
  timestamp timestamp NOT NULL,
  uri text NOT NULL,
  number INTEGER NOT NULL
);

COPY clicks(
  id,
  email,
  timestamp,
  uri,
  number
) FROM '/temp/data/clicks.csv' DELIMITER ',' CSV HEADER;