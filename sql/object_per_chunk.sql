\set VERBOSITY terse

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS results;
CREATE UNLOGGED TABLE results (data json);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'wal2json');

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (
a int PRIMARY KEY,
b text
);

-- By default don't write object chunks when write-in-chunks is true
CREATE TABLE x ();
DROP TABLE x;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'write-in-chunks', '1');

-- Ignore object-per-chunk when write-in-chunks is false
CREATE TABLE x ();
DROP TABLE x;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'write-in-chunks', '0', 'object-per-chunk', '1');

-- Writes chunks as valid json when both write-in-chunks and object-per-chunk are true
BEGIN;
INSERT INTO tbl(a, b) VALUES(1, 'first');
UPDATE tbl SET b='FIRST' WHERE a = 1;
DELETE FROM tbl WHERE a = 1;
SELECT 'msg1' FROM pg_logical_emit_message(true, 'wal2json', 'this is a transactional message');
SELECT 'msg2' FROM pg_logical_emit_message(false, 'wal2json', 'this is not a transactional message');
COMMIT;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'write-in-chunks', '1', 'object-per-chunk', '1');

-- All changes within a single transaction should have the same xid, timestamp, end_lsn when included
BEGIN;
INSERT INTO tbl(a, b) VALUES(4, 'fourth');
UPDATE tbl SET b='FOURTH' WHERE a = 4;
DELETE FROM tbl WHERE a = 4;
SELECT 'msg3' FROM pg_logical_emit_message(true, 'wal2json', 'this is a message');
COMMIT;
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'write-in-chunks', '1', 'object-per-chunk', '1', 'include-timestamp', '1', 'include-xids', '1', 'include-lsn', '1');

SELECT count(*) = 4, count(distinct data#>>'{xid}') = 1 FROM results;
SELECT count(*) = 4, count(distinct data#>>'{timestamp}') = 1 FROM results;
SELECT count(*) = 4, count(distinct data#>>'{nextlsn}') = 1 FROM results;
DELETE FROM results;

-- Changes not within a single transactions should have different xid, timestamp, end_lsn when included
--   also, non-transactional messages will not have transaction metadata
SELECT 'msg4' FROM pg_logical_emit_message(true, 'wal2json', 'this is a transactional message');
SELECT 'msg5' FROM pg_logical_emit_message(false, 'wal2json', 'this is not a transactional message');
INSERT INTO tbl(a, b) VALUES(5, 'fifth');
UPDATE tbl SET b='FIFTH' WHERE a = 5;
DELETE FROM tbl WHERE a = 5;
INSERT INTO results SELECT data::json FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'write-in-chunks', '1', 'object-per-chunk', '1', 'include-timestamp', '1', 'include-xids', '1', 'include-lsn', '1');

SELECT count(*) = 5, count(distinct data#>>'{xid}') = 4 FROM results;
SELECT count(*) = 5, count(distinct data#>>'{timestamp}') = 4 FROM results;
SELECT count(*) = 5, count(distinct data#>>'{nextlsn}') = 4 FROM results;
DELETE FROM results;

SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');
