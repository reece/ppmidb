>>> import duckdb
>>> schema_info = con.execute("DESCRIBE SELECT * FROM read_csv_auto('Vital_Signs.csv');").fetchall()
>>> schema_info
[('REC_ID', 'VARCHAR', 'YES', None, None, None),
 ('PATNO', 'BIGINT', 'YES', None, None, None),
 ('EVENT_ID', 'VARCHAR', 'YES', None, None, None),
 ('PAG_NAME', 'VARCHAR', 'YES', None, None, None),
 ('INFODT', 'VARCHAR', 'YES', None, None, None),
 ('WGTKG', 'DOUBLE', 'YES', None, None, None),
 ('HTCM', 'BIGINT', 'YES', None, None, None),
 ('TEMPC', 'DOUBLE', 'YES', None, None, None),
 ('BPARM', 'BIGINT', 'YES', None, None, None),
 ('SYSSUP', 'BIGINT', 'YES', None, None, None),
 ('DIASUP', 'BIGINT', 'YES', None, None, None),
 ('HRSUP', 'BIGINT', 'YES', None, None, None),
 ('SYSSTND', 'BIGINT', 'YES', None, None, None),
 ('DIASTND', 'BIGINT', 'YES', None, None, None),
 ('HRSTND', 'BIGINT', 'YES', None, None, None),
 ('ORIG_ENTRY', 'VARCHAR', 'YES', None, None, None),
 ('LAST_UPDATE', 'TIMESTAMP', 'YES', None, None, None)]