import matplotlib.pyplot as plt
import re
import pandas as pd
import json
import numpy as np
from matplotlib.patches import Patch
from matplotlib.lines import Line2D


plt.rcParams.update({'font.size': 20, 'hatch.linewidth': 2})

figsize = (30, 7)

# markers = ['o-', 'x-', '*-', '^-', '>-', '+-',
#            'o-', 'x-', '*-', '^-', '>-', '+-',
#            'o-', 'x-', '*-', '^-', '>-', '+-']


markers = ['o', '^', 's', '*']


dirs = {"FLICKR": "edit",
        "DBLP": "edit",
        "MIND": "edit",
        "YELP": "jaccard",
        "GDELT": "jaccard",
        "ENRON": "jaccard",
        }


# hatches = ['X', '\\', '-', '.', '+']
# colors = ['#ff7f0e', '#2ca02c', '#D62728', 'grey']

hatches = [' ', '\\', '.', 'X', '+']
colors = ['#2ca02c', '#ff7f0e', '#D62728', 'grey']

prop_cycle = plt.rcParams['axes.prop_cycle']
colors2 = prop_cycle.by_key()['color']


def export_legend(legend, filename="legend.png", expand=[-5,-5,5,5]):
    fig  = legend.figure
    fig.canvas.draw()
    bbox  = legend.get_window_extent()
    bbox = bbox.from_extents(*(bbox.extents + np.array(expand)))
    bbox = bbox.transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi="figure", bbox_inches=bbox)    
    
def save_legend_time(sub_methods, filename):
    fig = plt.figure()
    fig.add_subplot(111)
    
    leg = []
    for t, m in zip(sub_methods, markers):
        leg.append(Line2D([], [], color='#000', marker=m, fillstyle='none', 
                          linestyle='None', markersize=15, label=t))
            
    leg = fig.legend(handles=leg, ncol=len(leg), bbox_to_anchor=(1.5, 1.5))    
    export_legend(leg, filename)
    plt.show()    
    
def save_legend_cands(sub_methods, filename):
    fig = plt.figure()
    fig.add_subplot(111)
    
    filters = [('#f5ba87', 'CF (SM)'), ('#ff7f0e', 'NNF (SM)'),
               ('#6eb8eb', 'Basic (TJ)'), ('#3c8ec7', 'Positional (TJP)'),
               ('#1f77b4', 'Joint (TJPJ)'), 
               ('#fff', '//', 'VER'), ('#fff', '.', 'Matches')]

    leg = []
    for filt in filters:
        if len(filt) == 2:
            leg.append(Patch(facecolor=filt[0], alpha=1.0, label=filt[1], edgecolor='#000'))
        else:
            leg.append(Patch(facecolor=filt[0], alpha=1.0, hatch=filt[1], label=filt[2], edgecolor='#000'))
    
    leg = fig.legend(handles=leg, ncol=len(leg), bbox_to_anchor=(1.5, 1.5))    
    export_legend(leg, filename)
    plt.show()  
    
    
def prepare_times(l, sub_methods):
    rows = []
    for line in l:
        j = json.loads(line)
        if j['name'] not in sub_methods:
            continue
        final_j = {'Total': j['times']['total'], 'Method': j['name'], 'Size': j['size'] }
        if 'threshold' in j:
            final_j['Threshold'] = j['threshold']
        if 'k' in j:
            final_j['K'] = j['k']
    
        rows.append(final_j)
        
    max_size = max([row['Size'] for row in rows])
    for row in rows:
        row['Size'] = '{}%'.format(int(round(row['Size']/max_size, 2)*100))        
    return rows



def prepare_terms(l, sub_methods):
    rows = []
    for line in l:
        j = json.loads(line)
        if j['name'] not in sub_methods:
            continue
        final_j = {}
        
        if j['name'] == 'SM':
            final_j = {'N_CG' : j['terms']['candGen'], 'N_CF' : j['terms']['check_filter'],
                       'N_NNF' : j['terms']['nnf'], 'N_Matches' : j['terms']['total'],
                       'Method' : j['name'], 'Size' : j['size'], 'Threshold' : j['threshold']}
        else:
            final_j = {'N_CG' : j['terms']['candGen'], 'N_PR' : j['terms']['candRef'],
                       'N_REF' : j['terms']['verifiable'], 'N_Matches' : j['terms']['total'],
                       'Method' : j['name'], 'Size' : j['size'], 'Threshold' : j['threshold']}

        rows.append(final_j)
        
    max_size = max([row['Size'] for row in rows])
    for row in rows:
        row['Size'] = '{}%'.format(int(round(row['Size']/max_size, 2)*100))  
        
        
    final_rows = []
    rows2 = {}
    for row in rows:
        
        if row['Method'] == 'SM':
            final_rows.append(row)
            continue
        
        if (row['Size'], row['Threshold']) not in rows2:
            rows2[(row['Size'], row['Threshold'])] = []
        rows2[(row['Size'], row['Threshold'])].append(row)
        
        
    for row_list in rows2.values():
        row2 = {}
        row2['Method'] = 'TJ'
        row2['Size'] = row_list[0]['Size'] 
        row2['Threshold'] = row_list[0]['Threshold']   
        for row in row_list:
            row2[f"N_{row['Method']}"] = row['N_REF']
            row2['N_Matches'] = row['N_Matches']   
            row2['N_CG'] = row['N_CG']  
            row2['N_PR'] = row['N_PR']
        final_rows.append(row2)
        
    return final_rows



        
def plot_bar(row, no, no_methods, axes, linewidth=3):
    # total = row['P1']+row['P2']+row['Verification']
    if row['Method'] == 'SM':
        filters = [('#f5ba87', 'N_CG'), ('#ff7f0e', 'N_CF'), ('#ff7f0e', 'N_NNF')] 
    else:
        filters = [('#6eb8eb', 'N_PR'), ('#3c8ec7', 'N_TJB'), 
                  ('#1f77b4', 'N_TJP'), ('#1f77b4', 'N_TJPJ')]  #blue
        
    for c, filt in filters:
        axes.bar(no, row[filt], color=c,
                 #hatch=hatches[no % no_methods],
                 width=0.95, linewidth=linewidth, edgecolor='black')

    axes.bar(no, row[filters[-1][1]], color='#fff', width=0.95, hatch='\\',
              linewidth=linewidth, edgecolor='black')
    axes.bar(no, row['N_Matches'], color='#fff', width=0.95, hatch='.',
             linewidth=linewidth, edgecolor='black')
        
        
def fix_axes(axes, xticks, xticklabels, xlabel):
    axes.set_xticks(xticks)
    axes.set_xticklabels(xticklabels)
    axes.set_xlabel(xlabel)
    
    
def make_broken(ax0, ax1):
    ax0.spines['bottom'].set_visible(False)
    ax1.spines['top'].set_visible(False)
    ax0.xaxis.tick_top()
    ax0.tick_params(labeltop=False)  # don't put tick labels at the top
    ax1.xaxis.tick_bottom()
    # ax1.set_ylabel('Time (sec)')
    
    d = .015  # how big to make the diagonal lines in axes coordinates
    # arguments to pass to plot, just so we don't keep repeating them
    kwargs = dict(transform=ax0.transAxes, color='k', clip_on=False)
    ax0.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
    ax0.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal
    
    kwargs.update(transform=ax1.transAxes)  # switch to the bottom axes
    ax1.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax1.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal    
    
    
    
def plot_line(df, axes, xlim, xticks=None):
    for no, col in enumerate(df.columns):
        df[col].plot(ax=axes, legend=False, marker=markers[no], xlim=xlim,
                     color='#000', markersize=15, fillstyle='none')
        
    if xticks is not None:
        # axes.set_xticks(range(len(xticks)), xticks)
        axes.set_xticks(range(len(xticks)))
        axes.set_xticklabels(xticks)
    
    