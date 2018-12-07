\set VERBOSITY terse

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (id int);

DROP TABLE IF EXISTS results;
CREATE UNLOGGED TABLE results (data json);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'wal2json');

-- One row should have one record and one end_lsn
INSERT INTO tbl VALUES (1);
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-change-lsn', '1');

select count(*) = 1, count(distinct change->>'change_lsn') = 1
from results,
     json_array_elements(data->'change') change
;
DELETE FROM results;

-- Two rows should have two records and two change_lsns
INSERT INTO tbl VALUES (2);
INSERT INTO tbl VALUES (3);
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-change-lsn', '1');

select count(*) = 2, count(distinct change->>'change_lsn') = 2
from results,
     json_array_elements(data->'change') change
;
DELETE FROM results;

-- Two rows in one transaction should have one record and two change_lsns
INSERT INTO tbl VALUES (4), (5);
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-change-lsn', '1');

select count(distinct data) = 1, count(distinct change->>'change_lsn') = 2
from results,
     json_array_elements(data->'change') change
;
DELETE FROM results;

-- Two rows in one transaction with write in chunks should have two records and two change_lsn
INSERT INTO tbl VALUES (6), (7);
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-change-lsn', '1');

select count(*) = 2, count(distinct change->>'change_lsn') = 2
from results,
     json_array_elements(data->'change') change
;
DELETE FROM results;

-- Two rows in one COPY with should have two records and one change_lsn
COPY tbl FROM STDIN;
8
9
\.
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-change-lsn', '1');

select count(*) = 2, count(distinct change->>'change_lsn') = 1
from results,
     json_array_elements(data->'change') change
;
DELETE FROM results;

SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');

