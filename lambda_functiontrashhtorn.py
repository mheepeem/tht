import psycopg2
import os
import json
import boto3
import time

rekognition_client = boto3.client('rekognition')
iot_client = boto3.client('iot-data')
s3_client = boto3.client('s3')

device_id_='trash001' 



def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        file_name = os.path.splitext(key)[0]    
        file_telno = file_name[:10]
        print(f"file_telno: {file_telno}")

        # Process only image files
        if not key.lower().endswith(('.jpg', '.jpeg', '.png')):
            print("File is not an image, skipping...")

            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Deleted file from S3: {key}")

            return {
                'statusCode': 200,
                'body': json.dumps('Process Complete')
            }
        
        # Rekognition API call to detect labels
        try:
            response = rekognition_client.detect_labels(
                Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                MaxLabels=10,
                MinConfidence=75
            )
            
            labels = response['Labels']
            
            # Log detected labels
            print(f"Labels detected for {key}:")
            for label in response['Labels']:
                print(f"{label['Name']}: {label['Confidence']}% confidence")
                
            for label in response['Labels']:
                is_bottle = any(label['Name'] == 'Bottle' or label['Name'] == 'Water Bottle' or label['Name'] == 'Mineral Water' for label in labels) 
                print(f"is_bottle..{is_bottle}............")

                if is_bottle:
              
                    # ข้อมูลการเชื่อมต่อ PostgreSQL
                    host = 'tht3.cur0fk8d2h7a.us-east-1.rds.amazonaws.com'
                    database =  'postgres'
                    user = 'postgres'
                    password = 'admin1234'
                    port = '5432'

                    connection = psycopg2.connect(
                        host=host,
                        database=database,
                        user=user,
                        password=password
                    )
                        
                    cursor = connection.cursor()                    
                    cursor.execute("SELECT username,telno FROM public.trashhtorn_user WHERE telno = '" + file_telno + "'")
                    rows = cursor.fetchall()                
                    if cursor.rowcount > 0:
                        for row in rows:                            
                            username=row[0]
                            telno=row[1]
                            print(f"user row: {row[0]},{row[1]}")                        
                        #result = cursor.fetchone()
                    else:
                        username ="Guest"
                        telno=""
                    
                    #print(username)                      
                    sql = "INSERT INTO public.bottledetection (bucket, name, device_id, is_bottle, sample_time,username, point, telno) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
                    data = (bucket, key , device_id_, True, int(time.time()), username, 1, telno)
                    # Executing the INSERT statement
                    cursor.execute(sql, data)
                    
                    # Committing the transaction
                    connection.commit()

                    # ปิดการเชื่อมต่อ
                    cursor.close()
                    connection.close()
                                
                    # Print success message
                    print("Data inserted successfully!")  

                    message = {
                        'bottle': bucket,
                        'name': username,
                        'point': 1,
                        'sample_time': int(time.time()),  
                        'device_id': device_id_               
                    }
                    response = iot_client.publish(
                        topic='bottle_detect',
                        qos=1,
                        payload=json.dumps(message)
                    )
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps('Process IoTCore Complete')
                    }  
                                                                                               
                    break  # exit for loop label  
                else:
                # Delete the image from S3 if it's not a bottle
                    s3_client.delete_object(Bucket=bucket, Key=key)
                    print(f"Deleted image from S3: {key}")

                    
                    message = {
                        'bottle': "Not detect",  
                        'name': '',                     
                        'point': 0,
                        'sample_time': int(time.time()),  
                        'device_id': device_id_               
                    }
                    response = iot_client.publish(
                        topic='bottle_detect',
                        qos=1,
                        payload=json.dumps(message)
                    )
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps('Process IoTCore Complete')
                    }
            
                return {
                    'statusCode': 200,
                    'body': json.dumps('Process Complete')
                }

        except Exception as e:
            print(f"Error processing image with Rekognition: {e}")
            raise e
