import boto3
session = boto3.session.Session()

#Endpoint information
endpoint_url = "http://statistics.scalcia.com"
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

#Setting LifeCycle policy on bucket.
life_cycle_config = {
                    "Rules": [
                      {
                        "Expiration": {"Days": 10},
                        "Filter": {"Prefix": "demo"},
                        "Status": "Enabled",
                        "ID": "ExpiryPolicy-10Days-ObjectPrefix-demo"
                      },
                      {
                        "Expiration": {"Days": 30},
                        "Filter": {"Prefix": "object"},
                        "Status": "Enabled",
                        "ID": "ExpiryPolicy-1Month-ObjectPrefix-object"
                      }
                    ]
                  }

print "Configuring LifeCyclePolicy %s on bucket : %s"%(life_cycle_config, bucket)
s3c.put_bucket_lifecycle_configuration(Bucket=bucket,
                                       LifecycleConfiguration=life_cycle_config)

#Get LifeCyclePolicies configured on Bucket
print "Policies on bucket : %s"%(bucket)
print s3c.get_bucket_lifecycle_configuration(Bucket=bucket)

keys = ["demo_object.txt", "object_demo.txt", "employee_tata.txt"]
#File to upload to Objects endpoint
filename="/tmp/demo_object.txt"

for key in keys:
  print "Uploading file : %s as objectname : %s"%(filename, key)
  with open(filename, "r") as fh:
    s3c.put_object(Bucket=bucket, Key=key, Body=fh)
  print "object : %s, info : %s"%(key, s3c.head_object(Bucket=bucket, Key=key))

