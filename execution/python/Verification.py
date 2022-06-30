import sys
import pd
import os
from settings import dirs

if len(sys.argv) != 2:
    raise ValueError("Please give log and output folder")
log_dir = sys.argv[1]
    
times = {'small': {'TJPJ': 0, 'TJPJ - VU': 0, 'TJPJ - VUL': 0},
         'medium': {'TJPJ': 0, 'TJPJ - VU': 0, 'TJPJ - VUL': 0},
         'large': {'TJPJ': 0, 'TJPJ - VU': 0, 'TJPJ - VUL': 0}}
counts = {'small': 0, 'medium': 0, 'large': 0}

for name, dir in dirs.items():
    file = f'{log_dir}logs/experiment/threshold_verification/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    
    print("\n"+name)
    with open(file) as f:
        for no, line in enumerate(f.readlines()):
            if no % 100000 == 0:
                print(f"Line {no}\r", end='')
            
            if not line.startswith('bla'):
                continue
            l = line[:-1].split(',')
            
            size = (int(l[1]) + int(l[2])) / 2
            if size < 10:
                size = 'small'
            elif size < 100:
                size = 'medium'
            else:
                size = 'large'
            
            times[size]['TJPJ'] += float(l[3])
            times[size]['TJPJ - VU'] += float(l[4])
            times[size]['TJPJ - VUL'] += float(l[5])
            counts[size] += 1
print(times, counts)

(pd.DataFrame(times) / pd.Series(counts)).T * 1000000