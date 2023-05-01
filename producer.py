from kafka import KafkaProducer
import os, random

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

wd = '/home/Code/Tamrin 7'
csv_dir = 'uber_test_split'
csv_parts = [f for f in os.listdir(os.path.join(wd, csv_dir)) if f.endswith('.csv')]
csv_parts = [os.path.join(wd, csv_dir, f) for f in csv_parts if os.path.isfile(os.path.join(wd, csv_dir, f))]
data_list = []
for path in csv_parts:
    with open(path, 'r', encoding='utf-8') as fd:
        for line in fd:
            data_list.append(line)

random.shuffle(data_list)
for line in data_list:
    producer.send('uber', value=line.encode('utf-8'))

print(f'Sent {len(data_list)} lines in total.')
