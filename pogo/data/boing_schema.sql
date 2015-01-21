CREATE TABLE IF NOT EXISTS attempts (db_id INTEGER PRIMARY KEY AUTOINCREMENT,
 es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER,	bifrozt_host TEXT,
  source_ip TEXT, user TEXT, password TEXT, success INTEGER,
   country_code TEXT, country_name TEXT);

CREATE TABLE IF NOT EXISTS log_msg ( db_id INTEGER PRIMARY KEY AUTOINCREMENT,
 es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER,	bifrozt_host TEXT,
  server_info TEXT, message TEXT );


CREATE TABLE IF NOT EXISTS session_downloads (db_id  INTEGER PRIMARY KEY AUTOINCREMENT,
	 es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, filename TEXT NOT NULL,
	  contents TEXT );


CREATE TABLE IF NOT EXISTS session_recordings (db_id  INTEGER PRIMARY KEY AUTOINCREMENT,
	 es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, filename TEXT NOT NULL,
	  contents TEXT );

CREATE TABLE IF NOT EXISTS session_log_records (db_id  INTEGER PRIMARY KEY AUTOINCREMENT,
	 es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, channel TEXT, message TEXT );



