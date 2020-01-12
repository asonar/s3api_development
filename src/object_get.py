import boto3
session = boto3.session.Session()

#Endpoint information
endpoint_url = "http://objects007.scalcia.com"
access_key = "jOMpFFvMMw7un0UxBXRP2EhcVjkGza8n"
secret_key = "pzdBtAhr6Ak_o7IkfSavlxfhMSVMUUaL"

#Bucket, Objectname
bucket = "testbucket"
key = "employee_stats.txt"

#Connect to your Objects endpoint.
s3c = session.client(aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           endpoint_url=endpoint_url,
                           service_name="s3")


#Check if bucket exists and create it.
s3c.head_bucket(Bucket=bucket)
print "Bucket exists : %s"%(bucket)

#Verify if file exists
print "Checking if %s exists."%(key)
res = s3c.head_object(Bucket=bucket, Key=key)

#Read the object.
res = s3c.get_object(Bucket=bucket, Key=key)
data_stream = res["Body"]

datalen = 0
while True:
  #Read 64K at a time from stream.
  data = data_stream.read(64*1024)
  if not data:
    break

  datalen += len(data)
  print "Data read so far : %s"%(datalen)
print "Object %s read successfully. Total size : %sbytes"%(key, datalen)



