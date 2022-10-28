import boto3
import os
from dotenv import load_dotenv
load_dotenv()

def initTable():
    my_session = boto3.session.Session(
        aws_access_key_id=os.environ.get("ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("SECRET_KEY"),
        region_name = "us-east-1",
    )
    table_name = os.environ['TABLE_NAME']
    table = my_session.resource('dynamodb').Table(table_name)
    return table

table = initTable()

with open("data.csv") as file:
    next(file)
    index = 0
    for line in file:
        line = line.strip().split(",")
        try:
            table.put_item(Item={"id_": index, "max":line[0], "min":line[1]})
        except:
            print("error agregando dato", line)
        index+=1

