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

# convert csv files from cp1252 to utf-8
for f in *.csv; do iconv -c -f cp1252 -t utf-8 <$f >|xxx && mv -f xxx $f; done




gcloud alpha bigquery data-transfer create \
    --data-source=postgresql \
    --display-name="PPMI DTS 20250401b" \
    --target-dataset="20250401b" \
    --params='{
        "authentication.username": "postgres",
        "authentication.password": "hello2Erin",
        "database": "ppmidb",
        "endpoint.host": "ppmi-test:us-central1:reece-test",
        "endpoint.port": "5432",
        "schema": "public",
        "assets": "vital_signs",
        "sslMode": "DISABLE"
    }' \
    --service-account="service-100073959555@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com" \
    --location="us-central1" 