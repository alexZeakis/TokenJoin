import os
import pandas as pd
import matplotlib.pyplot as plt
from settings import prepare_terms, prepare_times
from settings import  save_legend_time, save_legend_cands 
from settings import plot_bar, plot_line, fix_axes, make_broken
from numpy import arange, nan
import sys

if len(sys.argv) != 2:
    raise ValueError("Please give log and output folder")
log_dir = sys.argv[1]


# plt.rcParams.update({'font.size': 20})


datasets = ['YELP', 'GDELT', 'ENRON', 'FLICKR', 'DBLP', 'MIND']
sub_methods = ["SM", "TJB", "TJP", "TJPJ"]
no_methods = len(sub_methods) + 1

deltas = [0.95, 0.90, 0.85, 0.80, 0.75, 0.70, 0.65, 0.60]
deltas2 = ['0.95', '0.90', '0.85', '0.80', '0.75', '0.70', '0.65', '0.60']
xlim = (0.955, 0.595)

no_axis = {(d, m):no for no, (d, m) in enumerate([(d, m) for d in deltas for m in sub_methods+['']])}
total_df = pd.DataFrame()
for pos, name in enumerate(datasets):
    # if pos!=0:
    #     continue
    file = f'{log_dir}logs/threshold_threshold/{name.lower()}.log'
    if not os.path.exists(file):
        continue

    
    with open(file) as f:
        rows = prepare_times(f.readlines(), sub_methods)
        rows = pd.DataFrame(rows)
        df = rows.pivot(index='Threshold', columns='Method', values='Total') 
        total_df = pd.concat([total_df, df])
        
        #for col in sub_methods[::-1]:
        #    df[col] = df[col] / df['TJB']
        # df = df[["TJB", "TJP", "TJPJ"]]

        for col in df:
            df.loc[df[col] > 18000, col] = nan
        
        fig, axes = plt.subplots(nrows=1, ncols=1)
        plot_line(df, axes, xlim)
        
        if pos == 0:
            axes.set_ylabel('Time (sec)')
        axes.set_xlabel(r'Threshold $\delta$')
        
        # axes.set_ylim(0, min(df.max().max(), 15000)+10)
        max_y = df.max().max() * 1.1
        axes.set_ylim(0, max_y)

    plt.savefig(f'{log_dir}/plots/threshold_time_{name}.pdf', bbox_inches='tight')

sub_methods2 = ["SM", "TJ", "TJP", "TJPJ"]
plt.show()
save_legend_time(sub_methods2, f'{log_dir}/plots/threshold_time_legend.pdf')

print(total_df)
# print((total_df['SM'] / total_df['TJPJ']).sort_values())
print((total_df['SM'] / total_df['TJPJ']).describe())
print((total_df['TJB'] / total_df['TJPJ']).describe())
print((total_df['TJP'] / total_df['TJPJ']).describe())


final_methods = ["SM", "TJ"]
deltas2 = ['', '0.90', '', '0.80', '', '0.70', '', '0.60']
no_methods = len(final_methods) + 1
no_axis = {(d, m):no for no, (d, m) in enumerate([(d, m) for d in deltas for m in final_methods+['']])}

logy = True

cands_stats = []
cands_stats2 = []
for pos, name in enumerate(datasets):
    # if pos!=2:
    #     continue
    file = f'{log_dir}logs/threshold_threshold/{name.lower()}.log'
    if not os.path.exists(file):
        continue
    with open(file) as f:
        fig, axes = plt.subplots(nrows=1, ncols=1)
        rows = prepare_terms(f.readlines(), sub_methods, norm=True)
        
        for row in rows:
            if (row['Threshold'], row['Method']) not in no_axis:
                continue
            
            if row['Time'] > 18000:
                continue
            
            no = no_axis[row['Threshold'], row['Method']]
            plot_bar(row, no, no_methods, axes)  
            if logy:
                axes.set_yscale('log')
                # axes.set_ylim(axes.get_ylim()[0], pow(10,5))
            
            
        fix_axes(axes, arange(0.5, len(deltas)*3+0.5, 3),
                  deltas2, r'Threshold $\delta$')
        if pos == 0:
            ylab = 'Num of Pairs'
            if logy:
                ylab += ' (log)'
            axes.set_ylabel(ylab)
            
        for row in rows:
            if row['Method'] == 'TJ':
                cands_stats.append((row['N_PR'], row['Method'], row['Threshold'], name))
                cands_stats2.append((row['N_TJPJ'], row['Method'], row['Threshold'], name))
                # cands_stats2.append((row['N_TJB'], row['N_TJP'], row['N_TJPJ']))
            else:
                cands_stats.append((row['N_CG'], row['Method'], row['Threshold'], name))
                cands_stats2.append((row['N_NNF'], row['Method'], row['Threshold'], name))
                
    plt.savefig(f'{log_dir}/plots/threshold_cands_{name}.pdf', bbox_inches='tight')
plt.show()
save_legend_cands(sub_methods, f'{log_dir}/plots/threshold_cands_legend.pdf')
        
cands_stats = pd.DataFrame(cands_stats).pivot(values=0, columns=1, index=(2, 3))
print((cands_stats['TJ'] / cands_stats['SM']).describe())

cands_stats2 = pd.DataFrame(cands_stats2).pivot(values=0, columns=1, index=(2, 3))
print((cands_stats2['TJ'] / cands_stats2['SM']).describe())

# cands_stats2 = pd.DataFrame(cands_stats2)
# print((1 - cands_stats2[1] / cands_stats2[0]).describe())
# print((1 - cands_stats2[2] / cands_stats2[0]).describe())
