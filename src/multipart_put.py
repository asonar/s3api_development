import boto3
session = boto3.session.Session()

endpoint_url = "http://objects007.scalcia.com"
access_key = "jOMpFFvMMw7un0UxBXRP2EhcVjkGza8n"
secret_key = "pzdBtAhr6Ak_o7IkfSavlxfhMSVMUUaL"

s3c = session.client(aws_access_key_id=access_key,
                     aws_secret_access_key=secret_key,
                     endpoint_url=endpoint_url, service_name="s3")

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

#Initiate multipart upload.
res = s3c.create_multipart_upload(Bucket=bucket, Key=key)

#Store UploadId in variable. This will be used when we start uploading parts.
uploadid = res["UploadId"]

#Part size
partsize = 5*1024*1024 #5MB

#Create file handle and upload the file to Objects endpoint.
print "Uploading file %s, as object %s in bucket %s"%(filename, key, bucket)

partnumber = 1
multiparts = []
with open(filename, "r") as fh:
  while True:
    data = fh.read(partsize)
    if not data:
      break
    print " - Uploading part_number %s, Size : %sMB"%(partnumber,
                                                      len(data)/(1024*1024))
    res = s3c.upload_part(Bucket=bucket, Key=key, Body=data, UploadId=uploadid,
                          PartNumber=partnumber)
    partinfo = {
      "PartNumber":partnumber,
      "ETag":res["ETag"],
    }
    multiparts.append(partinfo)
    partnumber += 1

#Complete multipart uploads
print "Finalizing object with parts :\n  -%s \n"%(multiparts)
s3c.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uploadid,
                              MultipartUpload={"Parts":multiparts})
#Verify if file is uploaded
print "Checking if %s exists."%(key)
print s3c.head_object(Bucket=bucket, Key=key)
