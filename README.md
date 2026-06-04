paramiko - sftp
psycopg - redshift
boto3 - aws & s3

The table names must be passed as parameters in SSM, during runtime.

To run rs_to_sftp_via_s3.py run:

python3 rs_to_s3_to_sftp.py \
    --table_name dev.sandbox.test_dqm_data \
    --s3_folder s3://alumis-analytics-sandbox-bucket/Alumis_Sandbox/VeevaCompass/test_bucket/


python sftp_connection_check.py