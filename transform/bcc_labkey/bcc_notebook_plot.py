import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def plot_all(events):

    df = pd.DataFrame(events)

    def bar_chart(df, type):
        df = df[df.type == type]
        df = df[['days_to_birth','type']]
        df = df.assign(Bin=lambda x: pd.cut(x.days_to_birth, bins=10, precision=0))
        df = df[['Bin','type']]
        df = df.rename(columns={"type": type, 'Bin': 'days_to_birth'})
        df.groupby(['days_to_birth']).count().plot(kind='bar')

    #fig, ax = plt.subplots()
    #groupby = df.groupby(['days_to_birth']).count().plot(kind='bar', ax=ax)
    #groupby = df.groupby(['Bin']).count().plot(kind='bar', ax=ax)
    # ticks = ax.set_xticks(ax.get_xticks()[::100])

    bar_chart(df, 'diagnosis')
    bar_chart(df, 'observation')
    bar_chart(df, 'sample')
    bar_chart(df, 'treatment')


    df = pd.DataFrame(events)


    fig, ax = plt.subplots()
    df[df.observation_type == 'weight_ohsu' ].plot(x ='days_to_birth', y='measurement', kind = 'scatter', title='weight', c='case_submitter_id', colormap='viridis', ax=ax)
    df[df.observation_type == 'height_ohsu' ].plot(x ='days_to_birth', y='measurement', kind = 'scatter', title='height', c='case_submitter_id', colormap='viridis' )
    df[df.observation_type == 'glycemic_lab_tests' ].plot(x ='days_to_birth', y='measurement', kind = 'scatter', title='glycemic', c='case_submitter_id', colormap='viridis' )
    df[df.observation_type == 'lesion_size' ].plot(x ='days_to_birth', y='measurement', kind = 'scatter', title='lesion (axis1)', c='case_submitter_id', colormap='viridis' )
    df[(df.observation_type == 'biomarker_measurement_ohsu') | (df.observation_type == 'biomarker_measurement_manually_entered')].plot(x ='days_to_birth', y='measurement', kind = 'scatter', title='biomarker', c='case_submitter_id', colormap='viridis' )



def plot_events_summary(events, case_submitter_id=None):
    sns.set(style="ticks", color_codes=True)
    # all events
    if not case_submitter_id:
        df = pd.DataFrame(events)
    else:
        df = pd.DataFrame([e for e in events if e.case_submitter_id == case_submitter_id])
    # count by type
    groups = df.groupby(['days_to_birth','type']).size().to_frame(name='size').reset_index()
    # make days_to_birth positive so it graphs left -> right
    groups['days_to_birth'] = groups['days_to_birth'].apply(lambda x: x*-1)

    chart = sns.scatterplot(x="days_to_birth", y="size", data=groups, hue="type" )
    legend = chart.legend(loc='center left', bbox_to_anchor=(1.25, 0.5), ncol=1)

def plot_events_details(events):
    sns.set(style="ticks", color_codes=True)
    # all events
    # count by type
    df = pd.DataFrame(events)
    groups = df.groupby(['days_to_birth','type']).size().to_frame(name='count').reset_index()
    # make days_to_birth positive so it graphs left -> right
    groups['days_to_birth'] = groups['days_to_birth'].apply(lambda x: x*-1)
    g = sns.FacetGrid(groups, col="type", height=4,col_wrap=3)
    g.map(plt.scatter, "days_to_birth",'count');
