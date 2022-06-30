import os
import pandas as pd
import matplotlib.pyplot as plt
from settings import prepare_terms, prepare_times
from settings import  save_legend_time, save_legend_cands 
from settings import plot_bar, plot_line, fix_axes, make_broken
from numpy import arange
import sys

if len(sys.argv) != 2:
    raise ValueError("Please give log and output folder")
log_dir = sys.argv[1]


limits = {  "FLICKR": ((3300, 17000), (0, 2050)),
            "DBLP": ((4100, 17000), (0, 2300)),
            "MIND": ((1500, 11500), (0, 750)),
            "YELP": None,
            "GDELT": ((11250, 17000), (100, 4500)),
            "ENRON": ((9200, 17000), (100, 3300)),
        }



plt.rcParams.update({'font.size': 20})


datasets = ['YELP', 'GDELT', 'ENRON', 'FLICKR', 'DBLP', 'MIND']
sub_methods = ["SM", "TJB", "TJP", "TJPJ"]
no_methods = len(sub_methods) + 1
sizes = ['20%', '40%', '60%', '80%', '100%']

xlim = (-0.05, 4.05)
xticks = sizes
no_axis = {(s, m):no for no, (s, m) in enumerate([(s, m) for s in sizes for m in sub_methods+['']])}

total_df = pd.DataFrame()

for pos, name in enumerate(datasets):
    
    file = f'{log_dir}logs/experiment/threshold_scalability/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    
    with open(file) as f:
        rows = prepare_times(f.readlines(), sub_methods)
        rows = pd.DataFrame(rows)
        df = rows.pivot(index='Size', columns='Method', values='Total')
        df = df.loc[sizes]
        
        if name in ['YELP']:
            fig, axes = plt.subplots(nrows=1, ncols=1)
            plot_line(df, axes, xlim, xticks)
            if pos == 0:
                axes.set_ylabel('Time (sec)')
            axes.set_xlabel(r'Dataset Size |$\mathcal{D}|$')
        else:        
            fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
            
            ax0, ax1 = axes[0], axes[1]
    
            plot_line(df, ax0, xlim, xticks)
            plot_line(df, ax1, xlim, xticks)
    
            lims = limits[name]
            if lims is not None:
                ax0.set_ylim(lims[0][0], lims[0][1])
                ax1.set_ylim(lims[1][0], lims[1][1])  
    
            make_broken(ax0, ax1)
            axes[1].set_xlabel(r'Dataset Size |$\mathcal{D}|$')
            
        total_df = pd.concat([total_df, df])

    plt.savefig(f'{log_dir}/plots/scalability_time_{name}.pdf', bbox_inches='tight')

#TODO: Remove this
sub_methods2 = ["SM", "TJ", "TJP", "TJPJ"]
plt.show()
save_legend_time(sub_methods2, f'{log_dir}/plots/scalability_time_legend.pdf')

print((total_df['SM'] / total_df['TJB']).describe())
print((total_df['TJB'] / total_df['TJP']).describe())
print((total_df['TJB'] / total_df['TJPJ']).describe())
    

final_methods = ["SM", "TJ"]
no_methods = len(final_methods) + 1
sizes = ['20%', '40%', '60%', '80%', '100%']

xlim = (-0.05, 4.05)
xticks = sizes
no_axis = {(s, m):no for no, (s, m) in enumerate([(s, m) for s in sizes for m in final_methods+['']])}

cands_stats = []
cands_stats2 = []
for pos, name in enumerate(datasets):
    # if pos!=0:
    #     continue
    file = f'{log_dir}logs/experiment/threshold_scalability/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    with open(file) as f:
        fig, axes = plt.subplots(nrows=1, ncols=1)
        rows = prepare_terms(f.readlines(), sub_methods)
        
        for row in rows:
            no = no_axis[row['Size'], row['Method']]
            plot_bar(row, no, no_methods, axes)  
            axes.set_yscale('log')
        fix_axes(axes, arange(0.5, len(sizes)*3+0.5, 3),
                  sizes, r'Dataset Size |$\mathcal{D}|$')
        if pos == 0:
            axes.set_ylabel('Num of Pairs (log)')
            
        for row in rows:
            if row['Method'] == 'TJ':
                cands_stats.append((row['N_PR'], row['Method'], row['Size'], name))
                cands_stats2.append((row['N_TJB'], row['N_TJP'], row['N_TJPJ']))
            else:
                cands_stats.append((row['N_CG'], row['Method'], row['Size'], name))
            
    plt.savefig(f'{log_dir}/plots/scalability_cands_{name}.pdf', bbox_inches='tight')
plt.show()
save_legend_cands(sub_methods, f'{log_dir}/plots/scalability_cands_legend.pdf')
        
cands_stats = pd.DataFrame(cands_stats).pivot(values=0, columns=1, index=(2, 3))
print((cands_stats['TJ'] / cands_stats['SM']).describe())

cands_stats2 = pd.DataFrame(cands_stats2)
print((1 - cands_stats2[1] / cands_stats2[0]).describe())
print((1 - cands_stats2[2] / cands_stats2[0]).describe())