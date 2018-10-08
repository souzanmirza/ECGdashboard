CREATE TABLE signal_samples (
    id serial PRIMARY KEY,
    batchnum int NOT NULL,
    signame varchar(50) NOT NULL,
    time timestamp NOT NULL,
    ecg1 float(1) NOT NULL,
    ecg2 float(1) NOT NULL,
    ecg3 float(1) NOT NULL);

--CREATE INDEX signal_samples_idx ON signal_samples (signame, time);

CREATE OR REPLACE FUNCTION create_partition_and_insert() RETURNS trigger AS
  $BODY$
    DECLARE
      partition_date TEXT;
      partition_time TEXT;
      partition TEXT;
    BEGIN
      partition_date := to_char(NEW.time,'YYYY_MM_DD_HH24_MI');
      partition := TG_RELNAME || '_' || partition_date;
      IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=partition) THEN
        RAISE NOTICE 'A partition has been created %',partition;
        EXECUTE 'CREATE TABLE ' || partition || ' (check (time = ''' || NEW.time || ''')) INHERITS (' || TG_RELNAME || ');';
        EXECUTE 'ALTER TABLE ' || partition || ' DROP CONSTRAINT IF EXISTS ' || partition || '_' || 'time_check;';
        EXECUTE 'ALTER TABLE ' || partition || ' ADD CONSTRAINT ' || partition || '_' || 'time_unique UNIQUE (time);';
        EXECUTE 'ALTER TABLE ' || partition || ' ADD CONSTRAINT ' || partition || '_' || 'id_unique UNIQUE (id);';
      END IF;
      EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_RELNAME || ' ' || quote_literal(NEW) || ').*;';
      RETURN NULL;
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

CREATE TRIGGER signal_samples_insert_trigger
BEFORE INSERT ON signal_samples
FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();

CREATE TABLE inst_hr (
    id serial PRIMARY KEY,
    batchnum int NOT NULL,
    time timestamp NOT NULL,
    signame varchar(50) NOT NULL,
    hr1 float(1) NOT NULL,
    hr2 float(1) NOT NULL,
    hr3 float(1) NOT NULL);

--CREATE INDEX inst_hr_idx ON inst_hr (batchnum, signame);

COMMIT;
