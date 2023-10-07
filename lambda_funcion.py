import json
import boto3
import csv


def lambda_handler(event, context):
    print (event)
    records = event['Records']
    for file in records:
        bucket_name = file['s3']['bucket']['name']
        file_name = file['s3']['object']['key']
    
        dir_file = '/tmp/' + file_name
        
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, file_name, dir_file)

        line_count = 0
        data_file_in = []
        with open(dir_file, mode='r') as csv_file:
            data_csv = csv.reader(csv_file,delimiter=';')
    
            # Transform
            for row in data_csv:
                data_file_in.append([row[0],row[1],row[2],row[3],row[4],row[5]])
        
        #Transform data
        balance = []
        for i in range(1,len(data_file_in)):
            balance.append(int(data_file_in[i][5])) 
            
        balance_max = max(balance)
        
        print (balance_max)
        balance_normalized = []
        balance_normalized.append('balance_normalized')
        for value in balance:
            balance_normalized.append(value/balance_max)

        
        
        #Load file
        file_out = "archivo_salida.txt"
        dir_file = '/tmp/' + file_out
        f = open(dir_file, "w")
        for i in range (0,len(data_file_in)):
            writer = csv.writer(f, delimiter=';')
            data_out = []
            for value in data_file_in[i]:
                data_out.append(value)
            data_out.append(balance_normalized[i])
            
            writer.writerow(data_out)
        f.close()
        
        bucket_out = 'curso-datos-bank'
        response = s3.upload_file(dir_file,bucket_out,file_out)
        
        
    return {
        'statusCode': 200,
        'body': json.dumps(str(response))
    }
