import os
import matplotlib.pyplot as plt
from settings import prepare_times, plot_line, save_legend_time
import sys
import pandas as pd

if len(sys.argv) != 2:
    raise ValueError("Please give log and output folder")
log_dir = sys.argv[1]


plt.rcParams.update({'font.size': 20})


datasets = ['YELP', 'GDELT', 'ENRON', 'FLICKR', 'DBLP', 'MIND']
sub_methods = ['OT', 'SMK', 'FJK', 'TJK']
no_methods = len(sub_methods) + 1


total_df = pd.DataFrame()

for pos, name in enumerate(datasets):
    if name != 'DBLP':
        ks = [500, 1000, 5000, 10000]    
    else:
        ks = [50, 100, 500, 1000]
    no_axis = {(k, m):no for no, (k, m) in enumerate([(k, m) for k in ks for m in sub_methods+['']])}
    file = f'{log_dir}logs/topk_k/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    with open(file) as f:
        rows = prepare_times(f.readlines(), sub_methods)
        rows = pd.DataFrame(rows)
        df = rows.pivot(index='K', columns='Method', values='Total')   
        df = df.loc[ks, sub_methods]
        total_df = pd.concat([total_df, df])
        
        fig, axes = plt.subplots(nrows=1, ncols=1)
        plot_line(df, axes, (0, int(ks[-1]*1.05)))
        if pos == 0:
            axes.set_ylabel('Time (sec)')
        axes.set_xlabel(r'Number of Results $k$')
        
    plt.savefig(f'{log_dir}/plots/topk_time_{name}.pdf', bbox_inches='tight')
plt.show()
save_legend_time(sub_methods, f'{log_dir}/plots/topk_time_legend.pdf')            
        # df = pd.DataFrame(rows).pivot(index=['Size', 'K'], columns='Method', values='total')
        # total_df = pd.concat([total_df, pd.concat({name: df}, names=['Dataset'])])

# plt.show()
print(total_df.apply(lambda x: x['SMK'] / x['TJK'], axis=1).describe())
print(total_df.apply(lambda x: x['FJK'] / x['TJK'], axis=1).describe())

