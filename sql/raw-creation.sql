CREATE TABLE raw(
	id SERIAL PRIMARY KEY,
	datetime VARCHAR(30) NOT NULL,
	latitude VARCHAR(30) NOT NULL,
	longitude VARCHAR(30) NOT NULL,
	pollutant VARCHAR(10) NOT NULL,
	value DECIMAL(5,2) NOT NULL,
	unit VARCHAR(30) NOT NULL
);

-- insert de teste

INSERT INTO raw (datetime, latitude, longitude, pollutant, value, unit)
VALUES ('2025-01-19T23:00:00Z','-1.464002035934475','-48.470622559253414','co',185.24,'parts_per_billion');