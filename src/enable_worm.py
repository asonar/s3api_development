import boto3
import json
session = boto3.session.Session()

#Endpoint information
endpoint_url = "http://objects.scalcia.com"
access_key = "Ty1H802tIvWlXrvKb_gAK8UrasmK69Nw"
secret_key = "Cvg3tyTMiYj-1Z8h0LfkYQDnw8zMuCLX"

#Bucket name
bucket = "bucket-from-api"
objname = "somekey"

#Connect to your Objects endpoint.
s3c = session.client(aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           endpoint_url=endpoint_url,
                           service_name="s3")

#Check if bucket exists and create it.
print "Creating bucket : %s"%(bucket)
s3c.create_bucket(Bucket=bucket)

#Enable versioning
print "Enabling versioning on bucket : %s"%(bucket)
versioning_config = {"Status":"Enabled"}
s3c.put_bucket_versioning(Bucket=bucket, VersioningConfiguration=versioning_config)

#Setting WORM on bucket.
worm_config = {
                "ObjectLockEnabled" : "Enabled",
                "Rule": {
                    "DefaultRetention": {
                        "Mode": "COMPLIANCE",
                        "Days":1
                    }
                }
              }

print "Configuring WORM  %s on bucket : %s"%(worm_config, bucket)
s3c.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration=worm_config)

#Get WORM configured on Bucket
print "WORM policies on bucket : %s"%(bucket)
print json.dumps(s3c.get_object_lock_configuration(Bucket=bucket), indent=4)

#Upload Object to bucket
res = s3c.put_object(Bucket=bucket, Key=objname)
versionid = res["VersionId"]

#Delete Object Version
print "Deleting object %s (version : %s)"%(objname, versionid)
s3c.delete_object(Bucket=bucket, Key=objname, VersionId=versionid)

