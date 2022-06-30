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


limits = {  "FLICKR": ((1000, 6100), (100, 450)),
            "DBLP": ((900, 8650), (0, 550)),
            "MIND": None,
            "YELP": None,
            "GDELT": ((5000, 17000), (150, 1250)),
            "ENRON": ((14000, 17000), (130, 1000)),
        }



plt.rcParams.update({'font.size': 20})


xlim = (0.855, 0.695)
datasets = ['YELP', 'GDELT', 'ENRON', 'FLICKR', 'DBLP', 'MIND']
sub_methods = ["SM", "TJB", "TJP", "TJPJ"]
no_methods = len(sub_methods) + 1
deltas = [0.85, 0.80, 0.75, 0.70]
deltas2 = ['0.85', '0.80', '0.75', '0.70']


no_axis = {(d, m):no for no, (d, m) in enumerate([(d, m) for d in deltas for m in sub_methods+['']])}
total_df = pd.DataFrame()
for pos, name in enumerate(datasets):
    # if pos!=0:
    #     continue
    file = f'{log_dir}logs/experiment/threshold_threshold/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    
    with open(file) as f:
        rows = prepare_times(f.readlines(), sub_methods)
        rows = pd.DataFrame(rows)
        df = rows.pivot(index='Threshold', columns='Method', values='Total')        
        
        if name in ['YELP', 'MIND']:
            fig, axes = plt.subplots(nrows=1, ncols=1)
            plot_line(df, axes, xlim)
            if pos == 0:
                axes.set_ylabel('Time (sec)')
            axes.set_xlabel(r'Threshold $\delta$')
        else:        
            fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
            
            ax0, ax1 = axes[0], axes[1]
    
            plot_line(df, ax0, xlim)
            plot_line(df, ax1, xlim)
    
            lims = limits[name]
            if lims is not None:
                ax0.set_ylim(lims[0][0], lims[0][1])
                ax1.set_ylim(lims[1][0], lims[1][1])  
    
            make_broken(ax0, ax1)
            axes[1].set_xlabel(r'Threshold $\delta$')
            
        total_df = pd.concat([total_df, df])

    plt.savefig(f'{log_dir}/plots/threshold_time_{name}.pdf', bbox_inches='tight')

#TODO: Remove this
sub_methods2 = ["SM", "TJ", "TJP", "TJPJ"]
plt.show()
save_legend_time(sub_methods2, f'{log_dir}/plots/threshold_time_legend.pdf')

print((total_df['SM'] / total_df['TJB']).describe())
print((total_df['TJB'] / total_df['TJP']).describe())
print((total_df['TJB'] / total_df['TJPJ']).describe())

    

final_methods = ["SM", "TJ"]
no_methods = len(final_methods) + 1
no_axis = {(d, m):no for no, (d, m) in enumerate([(d, m) for d in deltas for m in final_methods+['']])}

cands_stats = []
cands_stats2 = []
for pos, name in enumerate(datasets):
    # if pos!=0:
    #     continue
    file = f'{log_dir}logs/experiment/threshold_threshold/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    with open(file) as f:
        fig, axes = plt.subplots(nrows=1, ncols=1)
        rows = prepare_terms(f.readlines(), sub_methods)
        
        for row in rows:
            no = no_axis[row['Threshold'], row['Method']]
            plot_bar(row, no, no_methods, axes)  
            axes.set_yscale('log')
        fix_axes(axes, arange(0.5, len(deltas)*3+0.5, 3),
                  deltas2, r'Threshold $\delta$')
        if pos == 0:
            axes.set_ylabel('Num of Pairs (log)')
            
        for row in rows:
            if row['Method'] == 'TJ':
                cands_stats.append((row['N_PR'], row['Method'], row['Threshold'], name))
                cands_stats2.append((row['N_TJB'], row['N_TJP'], row['N_TJPJ']))
            else:
                cands_stats.append((row['N_CG'], row['Method'], row['Threshold'], name))
                
            
    plt.savefig(f'{log_dir}/plots/threshold_cands_{name}.pdf', bbox_inches='tight')
plt.show()
save_legend_cands(sub_methods, f'{log_dir}/plots/threshold_cands_legend.pdf')
        
cands_stats = pd.DataFrame(cands_stats).pivot(values=0, columns=1, index=(2, 3))
print((cands_stats['TJ'] / cands_stats['SM']).describe())


cands_stats2 = pd.DataFrame(cands_stats2)
print((1 - cands_stats2[1] / cands_stats2[0]).describe())
print((1 - cands_stats2[2] / cands_stats2[0]).describe())