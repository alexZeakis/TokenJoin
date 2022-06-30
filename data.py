import pandas as pd
import os

d = {}
for file in os.listdir('./data/'):
    if not file.endswith('csv'):
        continue
    print(file)
    
    with open('./data/'+file) as f:
        lengths = [len(line.split(';')) for line in f.readlines()]
        d[file] = pd.Series(lengths).describe().to_dict()
    
d = pd.DataFrame(d)