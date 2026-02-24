import boto3

#client = boto3.client('ec2')
#ami-0ecdd20ff07e019ed --for windows
#ami-03f4878755434977f ...for ubuntu
#ami-04708942c263d8190 ... for redhat

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html

# Specify your AWS credentials or use a configured profile

aws_region = 'ap-south-1'

# Create an EC2 client
ec2 = boto3.client('ec2', region_name=aws_region)

# Specify the AMI ID
ami_id = 'ami-04708942c263d8190'

# Specify other instance details
instance_type = 't2.micro'
key_name = 'goutami'

# Launch the instance
'''response = ec2.run_instances(
    ImageId=ami_id,
    InstanceType=instance_type,
    KeyName=key_name,
    MinCount=1,
    MaxCount=1
)
'''
# Extract instance ID from the response
response = ec2.terminate_instances(
    InstanceIds=[
        'i-0890b96e75004aba8'
    ]
)
