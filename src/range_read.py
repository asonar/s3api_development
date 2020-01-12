import boto3
session = boto3.session.Session()

#Endpoint information
endpoint_url = "http://objects007.scalcia.com"
access_key = "jOMpFFvMMw7un0UxBXRP2EhcVjkGza8n"
secret_key = "pzdBtAhr6Ak_o7IkfSavlxfhMSVMUUaL"

#Bucket, Objectname and Range read size
bucket = "testbucket"
key = "employee_stats.txt"
read_block_size = 64 * 1024 #64k read

s3c = session.client(aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           endpoint_url=endpoint_url,
                           service_name="s3")

#File to upload to Objects endpoint
filename="/tmp/employee_stats.txt"

#Check if bucket exists .
s3c.head_bucket(Bucket=bucket)
print "Bucket exists : %s"%(bucket)

#Create file handle and upload the file to Objects endpoint.
print "Uploading file %s, as object %s in bucket %s"%(filename, key, bucket)
with open(filename, "r") as fh:
  s3c.put_object(Bucket=bucket, Key=key, Body=fh)
#Verify if file is uploaded
print "Checking if %s exists."%(key)
res = s3c.head_object(Bucket=bucket, Key=key)

object_size = res["ContentLength"]
start_block_number = 0

#Find out how many blocks to read.
num_blocks_to_read = object_size/read_block_size

#Size of last block
last_block_size = object_size % read_block_size
if last_block_size > 0:
  num_blocks_to_read += 1

while num_blocks_to_read > 0:
  num_blocks_to_read -= 1
  range = "bytes=%s-%s"%(start_block_number, start_block_number+read_block_size)
  if num_blocks_to_read == 0 and last_block_size > 0:
    range = "bytes=%s-%s"%(start_block_number,
                           start_block_number+last_block_size)
  data = s3c.get_object(Bucket=bucket, Key=key,
                        Range=range)["Body"].read(read_block_size)
  print "Read object %s, range : %s, size of data : %s"%(key, range, len(data))
  start_block_number += read_block_size
print "Object %s read successfully"%(key)



