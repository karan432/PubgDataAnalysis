## sumaling game data produced in real time

import pandas as pd 
import subprocess
import time
from os import listdir
from os.path import isfile, join

#
mypath = "C:/Users/karan/OneDrive/Documents/BigData-Spring2019/Project/data/data"
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
print(len(onlyfiles))
onlyfiles.sort()
for file in onlyfiles:
    print(file)
    df = pd.read_csv("C:/Users/karan/OneDrive/Documents/BigData-Spring2019/Project/data/data/"+file)
    df = df.dropna(axis=0, subset=['player_name'])
    df.count()
    grouped = df.groupby('match_id')
    i = 0
    for name,group in grouped:
        gt = grouped.get_group(name)
        gt.to_csv('C:/Users/karan/OneDrive/Documents/BigData-Spring2019/Project/data/temp/names'+str(i)+'.csv', header=False)
        cmd = ["C:/opt/spark/kafka_2.12-2.2.0/bin/windows/kafka-console-producer.bat", "--broker-list" ,"localhost:9092", "--topic", "test"]
        f = open('C:/Users/karan/OneDrive/Documents/BigData-Spring2019/Project/data/temp/names'+str(i)+'.csv')
        out = subprocess.Popen(cmd, stdin=f, stdout=subprocess.PIPE)
        stdout = out.communicate()
        print(stdout)
        i += 1
    time.sleep(2)