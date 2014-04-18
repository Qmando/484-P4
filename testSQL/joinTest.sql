
CREATE TABLE DA (serial INTEGER, ikey INTEGER, filler CHAR(80), dkey DOUBLE);
CREATE TABLE DB (serial_id INTEGER, ikey INTEGER, filler_id CHAR(80), dkey_id DOUBLE);

-- load 100 tuples into the DA table
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (0, 100, '00000 string record',  0.0);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (1, 101, '00001 string record',  1.1);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (2, 102, '00002 string record',  2.2);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (3, 103, '00003 string record',  3.3);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (4, 104, '00004 string record',  4.4);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (5, 105, '00005 string record',  5.5);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (6, 106, '00006 string record',  6.6);


INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (200, 200, '00000 string record',  0.0);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (201, 205, '00001 string record',  1.1);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (202, 106, '00002 string record',  2.2);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (203, 19052, '00003 string record',  3.3);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (204, 24069, '00004 string record',  4.4);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (205, 17188, '00005 string record',  5.5);
INSERT INTO DB (serial_id, ikey, filler_id, dkey_id) VALUES (206, 28769, '00006 string record',  6.6);

-- Create Index DB on ikey
CREATE INDEX DB (ikey);

-- Use INL, Return 6-202, 3 records
SELECT DA.serial, DB.serial_id FROM DA, DB WHERE DA.ikey = DB.ikey; -- use INL

-- Create Index DA on ikey
CREATE INDEX DA (ikey);

-- Use INL, Return 6-202, 3 records - Indexes should not affect result
SELECT DA.serial, DB.serial_id FROM DA, DB WHERE DA.ikey = DB.ikey;

-- Use SNL, Return NOTHING
SELECT * FROM DA, DB WHERE DA.ikey > DB.ikey;

-- Use SNL, Return 100(200 - 206), 101(200-206)... 106(200-206), 49 records - Indexes should not affect result
SELECT DA.ikey, DB.serial_id FROM DA, DB WHERE DA.ikey < DB.serial_id;

-- Drop the indexes in order to run SMJ
DROP INDEX DA (ikey);
DROP INDEX DB (ikey);


-- Use SMJ, Return 100(200 - 206), 101(200-206)... 106(200-206), 49 records - Should be same result as SNL join above
-- SELECT DA.ikey, DB.serial_id FROM DA, DB WHERE DA.ikey < DB.serial_id;

-- Use SNL, should return 49 records with ALL 8 columns
SELECT * FROM DA, DB WHERE DA.ikey < DB.serial_id;

DROP TABLE DA;
DROP TABLE DB;


-- CREATE NEW TABLES to test further

CREATE TABLE DA (key_id INTEGER, value INTEGER);
CREATE TABLE DB (key_id INTEGER, value_db INTEGER);

INSERT INTO DA (key_id, value) VALUES (1, 100);
INSERT INTO DA (key_id, value) VALUES (2, 200);
INSERT INTO DA (key_id, value) VALUES (3, 300);

INSERT INTO DB (key_id, value_db) VALUES (4, 400);
INSERT INTO DB (key_id, value_db) VALUES (5, 500);
INSERT INTO DB (key_id, value_db) VALUES (6, 600);

-- Create Index DB on key
CREATE INDEX DB (key_id);

-- Run INL, Return NOTHING
SELECT DA.key_id, DB.key_id FROM DA, DB WHERE DA.key_id = DB.key_id;

--Run SNL, Return NOTHING
SELECT DA.key_id, DB.key_id FROM DA, DB WHERE DA.value > DB.value_db;


-- Drop tables and indexes
DROP INDEX DB (key_id);
DROP TABLE DA;
DROP TABLE DB;



