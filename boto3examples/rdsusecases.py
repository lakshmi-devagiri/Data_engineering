import boto3
from time import sleep
client = boto3.client('rds',region_name="ap-south-1")
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html
'''response = client.create_db_instance(
    DBName='mysqldatabase123',
    DBInstanceIdentifier='dec27mysql',
    AllocatedStorage=20,
    DBInstanceClass='db.t3.micro',
    Engine='mysql',
    MasterUsername='myuser',
    MasterUserPassword='mypassword'
)'''
response = client.delete_db_instance(
    DBInstanceIdentifier='dec27mysql',
    SkipFinalSnapshot=True,
    DeleteAutomatedBackups=True
)

sleep(10)

response = client.describe_db_instances()
print(response)
for db_instance in response['DBInstances']:
    db_instance_identifier = db_instance['DBInstanceIdentifier']
    db_instance_status = db_instance['DBInstanceStatus']
    db_instance_engine = db_instance['Engine']

    print(f"DB Instance Identifier: {db_instance_identifier}")
    print(f"Status: {db_instance_status}")
    print(f"Engine: {db_instance_engine}")
    print("\n")