
CREATE TABLE DA (serial INTEGER, ikey INTEGER, filler CHAR(80), dkey DOUBLE);
CREATE TABLE DB (serial INTEGER, ikey INTEGER, filler CHAR(80), dkey DOUBLE);

-- load 100 tuples into the DA table
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (0, 100, '00000 string record',  0.0);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (1, 101, '00001 string record',  1.1);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (2, 102, '00002 string record',  2.2);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (3, 103, '00003 string record',  3.3);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (4, 104, '00004 string record',  4.4);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (5, 105, '00005 string record',  5.5);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (6, 106, '00006 string record',  6.6);


INSERT INTO DB (serial, ikey, filler, dkey) VALUES (200, 100, '00000 string record',  0.0);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (201, 105, '00001 string record',  1.1);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (202, 106, '00002 string record',  2.2);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (203, 19052, '00003 string record',  3.3);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (204, 24069, '00004 string record',  4.4);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (205, 17188, '00005 string record',  5.5);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (206, 28769, '00006 string record',  6.6);

-- Create Index DB on ikey
CREATE INDEX DB (ikey);

-- Run SELECT (should be indexSelect) Return only 0
SELECT DB.serial FROM DB WHERE DB.ikey = 100;

-- Run SELECT (should be scanSelect) Return 1 - 6
SELECT DB.serial FROM DB WHERE DB.ikey > 100;

-- Run SELECT (should be ScanSelect) Return 200
SELECT DA.serial FROM DA WHERE DA.ikey = 100;

-- Run SELECT (should be ScanSelect) Return 201 - 206
SELECT DA.serial FROM DA WHERE DA.ikey > 100;

-- Run SELECT (should be ScanSelect) Return 0 - 6
SELECT DA.serial, DA.filler, DA.dkey FROM DA WHERE DA.ikey >= 100;

-- Run SELECT (should be ScanSelect) Return 200 - 206
SELECT DA.serial, DA.filler, DA.dkey FROM DA WHERE DA.ikey >= 100;

-- Run SELECT (Should be ScanSelect) Return everything in DA
SELECT * FROM DA;

-- Run SELECT (Should be ScanSelect) Return everything in DB
SELECT * FROM DB; 

-- Drop all of the indexes and tables
DROP INDEX DB (ikey);
DROP TABLE DA;
DROP TABLE DB;
