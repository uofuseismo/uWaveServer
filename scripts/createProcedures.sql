--- psql -f createProcedures.sql --dbname=uwsltutahdb

--- Creates the stream table when no schema is provided.
--- The result will look like network_station_data
CREATE OR REPLACE FUNCTION create_stream_data_table_name(
  IN network TEXT,  --- The network code - e.g., UU
  IN station TEXT   --- The station code - e.g., FORK
  )
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  RETURN FORMAT('%s_%s_data', LOWER(network), LOWER(station));

--- Creates the stream table when a schema is provided.
--- The result will look like network_station_data
CREATE OR REPLACE FUNCTION create_stream_data_table_name_in_schema(
  IN schema  TEXT,  --- The schema - e.g., ynp or utah
  IN network TEXT,  --- The network code - e.g., UU
  IN station TEXT   --- The station code - e.g., FORK
  )
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  RETURN FORMAT('%s.%s_%s_data', schema, LOWER(network), LOWER(station));

--- Adds the network, station, channel, and location_code to the stream table.
--- e.g., SET search_path = utah, PUBLIC;
---       CALL update_streams_table('WY', 'fake', 'HHZ', '01', 'streams', 'ynp.wy_fake_data');
---CREATE OR REPLACE PROCEDURE update_streams_table(
---  schema TEXT,                 --- The schema - e.g., utah
---  network TEXT,                --- The network code - e.g., UU
---  station TEXT,                --- The station name - e.g., FORK
---  channel TEXT,                --- The channel code - e.g., HHZ
---  location_code TEXT,          --- The location code - e.g., 01
---  stream_table_name TEXT,      --- The stream data table name - e.g., streams
---  stream_data_table_name TEXT  --- The stream data table name - e.g., uu_fork_data
---  )
---  LANGUAGE plpgsql AS
---$func$
---BEGIN
  ---EXECUTE FORMAT('INSERT INTO %I (network, station, channel, location_code, data_table_name)
---                  VALUES ($1, $2, $3, $4, $5)
---                  ON CONFLICT (network, station, channel, location_code, data_table_name) DO NOTHING',
     ---stream_table_name
---  )
---  USING UPPER(network), UPPER(station), UPPER(channel), UPPER(location_code), stream_data_table_name;
---END
---$func$;

--- Adds the network, station, channel, and location_code to the stream table.
--- e.g., CALL update_streams_table('WY', 'fake', 'HHZ', '01', 'ynp.wy_fake_data');
CREATE OR REPLACE PROCEDURE update_streams_table(
  IN v_network TEXT,                --- The network code - e.g., UU
  IN v_station TEXT,                --- The station name - e.g., FORK
  IN v_channel TEXT,                --- The channel code - e.g., HHZ
  IN v_location_code TEXT,          --- The location code - e.g., 01
  IN v_stream_data_table_name TEXT  --- The table name - e.g., uu_fork_data
  )
  LANGUAGE plpgsql AS
$func$
BEGIN
  --- SELECT create_stream_data_table_name(v_network, v_station) INTO stream_data_table_name;
  INSERT INTO streams(network, station, channel, location_code, data_table_name)
         VALUES (UPPER(v_network), UPPER(v_station), UPPER(v_channel), UPPER(v_location_code), v_stream_data_table_name)
         ON CONFLICT (network, station, channel, location_code, data_table_name) DO NOTHING;
END
$func$;

--- Adds the network, station, channel, and location_code to the stream table.
--- e.g., SET search_path = ynp;
---       CALL update_streams_table('ynp', 'WY', 'fake', 'HHZ', '01');
CREATE OR REPLACE PROCEDURE update_streams_table_in_schema(
  IN v_schema TEXT,                 --- The schema - e..g., utah
  IN v_network TEXT,                --- The network code - e.g., UU
  IN v_station TEXT,                --- The station name - e.g., FORK
  IN v_channel TEXT,                --- The channel code - e.g., HHZ
  IN v_location_code TEXT,          --- The location code - e.g., 01
  IN v_stream_data_table_name TEXT  --- The table name - e.g., utah.uu_fork_data
  )
  LANGUAGE plpgsql AS
$func$
BEGIN
  --- SELECT public.create_stream_data_table_name_with_schema(v_schema, v_network, v_station)
  ---   INTO stream_data_table_name;
  INSERT INTO streams(network, station, channel, location_code, data_table_name)
     VALUES (UPPER(v_network), UPPER(v_station), UPPER(v_channel), UPPER(v_location_code), v_stream_data_table_name)
     ON CONFLICT (network, station, channel, location_code, data_table_name) DO NOTHING;
END
$func$;


--- This function creates the stream data table.
--- e.g., SET search_path = utah, PUBLIC;
---       CALL create_stream_data_table('UU', 'FORK', 'HHZ', '01');
CREATE OR REPLACE PROCEDURE create_stream_data_table_work(
  v_network TEXT,                  --- The network code - e.g., UU
  v_station TEXT,                  --- The station name - e.g., FORK
  v_channel TEXT,                  --- The channel code - e.g., HHZ
  v_location_code TEXT,              --- The location code - e.g., 01
  v_duration_interval_mus BIGINT,    --- The chunk duration in microseconds
  v_retention_interval_mus BIGINT,   --- The duration to retain data in the table in microseconds
  v_compression_interval_mus BIGINT, --- Chunks older than this duration are compressed
  v_stream_data_table_name TEXT      --- The stream data table name. 
  )
  LANGUAGE plpgsql AS
$func$
DECLARE
  stream_data_table_name TEXT;
BEGIN
  --- SET timescaledb.enable_chunk_skipping TO ON;

  --- N.B. the start_time is the first column with a timestamp so it 
  ---      is the default partition.  Also, the columnstore is enabled.
  --- EXECUTE 'CALL remove_columnstore_policy(''' || v_stream_data_table_name || ''', if_exists => true)';

  EXECUTE
     'CREATE TABLE IF NOT EXISTS '
     || v_stream_data_table_name || 
     '(
        stream_identifier INTEGER NOT NULL,
        start_time TIMESTAMPTZ NOT NULL,
        end_time TIMESTAMPTZ NOT NULL CHECK(end_time >= start_time),
        load_time TIMESTAMPTZ DEFAULT NOW(),
        sampling_rate DOUBLE PRECISION NOT NULL CHECK(sampling_rate > 0),
        number_of_samples INT NOT NULL CHECK(number_of_samples >= 0),
        little_endian BOOLEAN NOT NULL,
        compressed BOOLEAN NOT NULL,
        data_type CHARACTER (1) NOT NULL CHECK(data_type IN (''i'', ''f'', ''d'', ''l'', ''t'')),
        data BYTEA NOT NULL,
        PRIMARY KEY (stream_identifier, start_time),
        FOREIGN KEY (stream_identifier) REFERENCES streams(identifier)
     )';

  --- Create the hypertable with chunks at the given interval based on the start time
  EXECUTE 'SELECT public.create_hypertable( ''' || v_stream_data_table_name ||
          ''', public.by_range(''start_time'',' || v_duration_interval_mus || '), if_not_exists => TRUE)';
  --- Frequently will query by a stream ID so speed that up
  EXECUTE 'ALTER TABLE ' || v_stream_data_table_name ||
     ' SET(timescaledb.enable_columnstore, timescaledb.segmentby = ''stream_identifier'', timescaledb.orderby = ''start_time'')';
  --- We want to do compression
  EXECUTE 'SELECT public.add_compression_policy(''' || v_stream_data_table_name ||''', compress_after => INTERVAL ''24 hours'', if_not_exists => TRUE)';
  --- And we want to drop data after a certain amount of time
  EXECUTE 'SELECT public.add_retention_policy(''' || v_stream_data_table_name || ''', drop_after => INTERVAL '''
          || v_retention_interval_mus || ' microseconds'', if_not_exists => TRUE)';

  --- USING v_retention_interval_mus;
  --- Should be done with ALTER PRIVILEGES
  --- EXECUTE FORMAT('GRANT SELECT ON %I TO uws_read_only', table_name);

END
$func$;


--- This function creates the stream data table.  This is used when using schemas.
--- e.g., SET search_path = utah, PUBLIC;
---       CALL create_stream_data_table('utah', 'UU', 'FORK', 'HHZ', '01', 21300000, 86400000);
CREATE OR REPLACE PROCEDURE create_stream_data_table_in_schema(
  IN v_schema  TEXT,                   --- The schema - e.g., utah
  IN v_network TEXT,                   --- The network code - e.g., UU
  IN v_station TEXT,                   --- The station name - e.g., FORK
  IN v_channel TEXT,                   --- The channel code - e.g., HHZ
  IN v_location_code TEXT,             --- The location code - e.g., 01
  IN v_duration_interval_mus BIGINT,   --- The chunk duration in microseconds
  IN v_retention_interval_mus BIGINT,  --- The duration to retain data in the table in microseconds
  IN v_compression_interval_mus BIGINT --- The duration after which data is compressed in microseconds
  )
  LANGUAGE plpgsql AS
$func$
DECLARE
  stream_data_table_name TEXT;
BEGIN

  SELECT public.create_stream_data_table_name_in_schema(v_schema, v_network, v_station)
     INTO stream_data_table_name;

  CALL public.create_stream_data_table_work(
     v_network,
     v_station,
     v_channel,
     v_location_code,
     v_duration_interval_mus,
     v_retention_interval_mus,
     v_compression_interval_mus,
     stream_data_table_name 
  );

  CALL public.update_streams_table_in_schema(
     v_schema,
     v_network,
     v_station,
     v_channel,
     v_location_code,
     stream_data_table_name);
END
$func$;

--- This function creates the stream data table - this is used when not
--- using schemas.
--- e.g., CALL create_stream_data_table('UU', 'FORK', 'HHZ', '01', 21300000, 86400000);
CREATE OR REPLACE PROCEDURE create_stream_data_table(
  IN v_network TEXT,                    --- The network code - e.g., UU
  IN v_station TEXT,                    --- The station name - e.g., FORK
  IN v_channel TEXT,                    --- The channel code - e.g., HHZ
  IN v_location_code TEXT,              --- The location code - e.g., 01
  IN v_duration_interval_mus BIGINT,    --- The chunk duration in microseconds
  IN v_retention_interval_mus BIGINT,   --- The duration to retain data in the table in microseconds
  IN v_compression_interval_mus BIGINT  --- Compress after this duration in microseconds
  )
  LANGUAGE plpgsql AS
$func$
DECLARE
  stream_data_table_name TEXT;
BEGIN

  SELECT create_stream_data_table_name(v_network, v_station)
     INTO stream_data_table_name;

  CALL create_stream_data_table_work(
     v_network,
     v_station,
     v_channel,
     v_location_code,
     v_duration_interval_mus,
     v_retention_interval_mus,
     v_compression_interval_mus,
     stream_data_table_name
  );

  CALL update_streams_table_in_schema(
     v_schema,
     v_network,
     v_station,
     v_channel,
     v_location_code,
     stream_data_table_name);
END
$func$;


CREATE OR REPLACE PROCEDURE create_stream_data_table_with_defaults(
  IN v_network TEXT,                --- The network code - e.g., UU
  IN v_station TEXT,                --- The station name - e.g., FORK
  IN v_channel TEXT,                --- The channel code - e.g., HHZ
  IN v_location_code TEXT           --- The location code - e.g., 01
  )
  LANGUAGE plpgsql AS
$func$
DECLARE
  stream_table_name TEXT;
  duration_interval_mus BIGINT;
  retention_interval_mus BIGINT;
  compression_interval_mus BIGINT;
BEGIN
  SELECT settings.duration_interval_mus,
         settings.retention_interval_mus,
         settings.compression_interval_mus
     INTO duration_interval_mus, retention_interval_mus, compression_interval_mus FROM settings;

  CALL create_stream_data_table(v_network,
                                v_station,
                                v_channel,
                                v_location_code,
                                duration_interval_mus,
                                retention_interval_mus,
                                compression_interval_mus);
END
$func$;

CREATE OR REPLACE PROCEDURE create_stream_data_table_with_defaults_in_schema(
  IN v_schema TEXT,                 --- The schema - e.g., utah
  IN v_network TEXT,                --- The network code - e.g., UU
  IN v_station TEXT,                --- The station name - e.g., FORK
  IN v_channel TEXT,                --- The channel code - e.g., HHZ
  IN v_location_code TEXT           --- The location code - e.g., 01
  )
  LANGUAGE plpgsql AS
$func$
DECLARE
  stream_table_name TEXT;
  l_duration_interval_mus BIGINT;
  l_retention_interval_mus BIGINT;
  l_compression_interval_mus BIGINT;
BEGIN
  SELECT settings.duration_interval_mus,
         settings.retention_interval_mus,
         settings.compression_interval_mus
  INTO l_duration_interval_mus,
       l_retention_interval_mus,
       l_compression_interval_mus
  FROM settings;

  CALL public.create_stream_data_table_in_schema(
     v_schema, 
     v_network,
     v_station,
     v_channel,
     v_location_code,
     l_duration_interval_mus,
     l_retention_interval_mus,
     l_compression_interval_mus);
END
$func$;
