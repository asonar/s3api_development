import boto3
session = boto3.session.Session()

endpoint_url = "http://objects007.scalcia.com"
access_key = "jOMpFFvMMw7un0UxBXRP2EhcVjkGza8n"
secret_key = "pzdBtAhr6Ak_o7IkfSavlxfhMSVMUUaL"

s3c = session.client(aws_access_key_id=access_key,
                     aws_secret_access_key=secret_key,
                     endpoint_url=endpoint_url,
                     service_name="s3")

#File to upload to Objects endpoint
filename="/tmp/employee_stats.txt"

#Declaring bucket name.
bucket = "testbucket"

#"/tmp/employee_stats.txt" will be uploaded as objectname=employee_stats.txt
key = "employee_stats.txt"

#Check if bucket exists and create it.
try:
  s3c.head_bucket(Bucket=bucket)
  print "Bucket already exists : %s"%(bucket)
except Exception as err:
  print "Creating bucket %s"%(bucket)
  s3c.create_bucket(Bucket=bucket)

#Create file handle and upload the file to Objects endpoint.
print "Uploading file %s, as object %s in bucket %s"%(filename, key, bucket)
with open(filename, "r") as fh:
  s3c.put_object(Bucket=bucket, Key=key, Body=fh)

#Verify if file is uploaded
print "Checking if %s exists."%(key)
print "Head Object Response : %s"%(s3c.head_object(Bucket=bucket, Key=key))
