#boto3 used to connect aws resources ... using python language.  pip install boto3
import boto3

client = boto3.client('rds', region_name='ap-south-1')

response = client.create_db_instance(
    DBName='mysqldb',
    DBInstanceIdentifier='boto3pocs',
    AllocatedStorage=20,
    DBInstanceClass='db.t3.micro',
    Engine='mysql',
    MasterUsername='myuser',
    MasterUserPassword='mypassword')
