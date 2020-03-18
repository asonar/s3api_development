import boto3
session = boto3.session.Session()

#Endpoint information
endpoint_url = "http://Objects.scalcia.com"
access_key = "bVSjIccNo738F5VmpaYjA4BTV_J7h_YH"
secret_key = "_rCf_LXRPea1j9xmuCWKlFsWqu7Qa_1n"

#Bucket name
bucket = "bucket-from-api"

#Connect to your Objects endpoint.
s3c = session.client(aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           endpoint_url=endpoint_url,
                           service_name="s3")


#Check if bucket exists and create it.
print "Creating bucket : %s"%(bucket)
try:
  s3c.head_bucket(Bucket=bucket)
except Exception as err:
  s3c.create_bucket(Bucket=bucket)

#Enable versioning
print "Enabling versioning on bucket : %s"%(bucket)
versioning_config = {"Status":"Enabled"}
s3c.put_bucket_versioning(Bucket=bucket, VersioningConfiguration=versioning_config)

#Get versioning status of  Bucket
print "Versioning status of bucket : %s"%(bucket)
s3c.get_bucket_versioning(Bucket=bucket)

#Upload 3 versions
objname = "moviename"
movies = ["The Dark Knight","Star Wars", "The God Father"]
for movie in movies:
  print s3c.put_object(Bucket=bucket, Key=objname, Body=movie)
  print "*"*10
