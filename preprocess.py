"""
process data set
1.find each inst belong to which app
2.find initial state all conflicts conditions, for migrating
"""

from collections import Counter, defaultdict
import pandas as pd

import pickle

def loaddata():
    inst_path = "./data/instance_deploy_20180606.csv"
    machine_path = "./data/machine_resources_20180606.csv"
    app_resrc_path = "./data/app_resources_20180606.csv"
    app_infer_path = "./data/app_interference_20180606.csv"

    # laoding data
    path_df = open(inst_path,'r')
    try:
        inst_df = pd.read_csv(path_df, index_col=False,names=['inst_id','app_id','machine_id'])
    finally:
        path_df.close()

    path_df = open(app_resrc_path, 'r')
    try:
        app_resrc_df = pd.read_csv(path_df, index_col=False,
                                   names=['app_id', 'cpu', 'mem', 'disk', 'P', 'M', 'PM'])
        app_resrc_df.set_index(['app_id'], inplace=False)
    finally:
        path_df.close()

    path_df = open(app_infer_path,'r')
    try:
        app_infer_df = pd.read_csv(app_infer_path, index_col=False, names=['app_id1', 'app_id2', 'volum'])
        app_infer_df.set_index(['app_id1', 'app_id2'], inplace=True) # set two column indexs
    finally:
        path_df.close()

    path_df = open(machine_path,'r')
    try:
        machine_df = pd.read_csv(machine_path,index_col=False, names=['machine_id', 'cpu', 'mem', 'disk', 'P', 'M', 'PM'])
        machine_df.set_index(['machine_id'], inplace=True)
    finally:
        path_df.close()

    return inst_df, app_resrc_df, machine_df, app_infer_df


# check initial state conflicts, there 4 constraints
'''
1. instances' resources(cpu,mem,disk) in machine can't exceed the machine's volume
2. instances in machine can't violate the app_interference
3. in same machine, the number of instance of P class, M class, PM class can't exceed the machine's volume
4. one instance only can be allocated to one machine
'''

def checkInitialConflict(inst_df, app_resrc_df, machine_df, app_infer_df):
    # step 1 find vilating app_interference constraint instance for magrating them later
    #machine_conflict = []
    inst_conflict = defaultdict(list)

    # merge inst and app
    inst_app_df = pd.merge(app_resrc_df, inst_df, on=['app_id'], how='left')
    inst_app_df = inst_app_df[['app_id', 'inst_id', 'machine_id', 'cpu', 'mem', 'disk', 'P', 'M', 'PM']]
    inst_app_df.set_index(['inst_id'], inplace=True)
    # inst_app_df.to_csv('./data/inst_app_df.csv',columns=['app_id','inst_id','machine_id','cpu','mem','disk','P','M','PM'],header=False,mode='a')

    # split cpu&mem
    cpu = inst_app_df['cpu'].str.split('|', expand=True).astype(float)  # str convert to float
    mem = inst_app_df['mem'].str.split('|', expand=True).astype(float)  # str convert to float
    cpu_list = ['cpu' + str(i) for i in range(98)]
    mem_list = ['mem' + str(i) for i in range(98)]
    cpu.columns = cpu_list
    mem.columns = mem_list
    # join cpu&mem
    inst_app_df = inst_app_df.join(cpu)
    inst_app_df = inst_app_df.join(mem)
    del inst_app_df['cpu']
    del inst_app_df['mem']

    inst_group = inst_df.groupby(['machine_id'], sort=False) # group instances in same machine
    print("check initial conflict begin!")
    for machine_id, group in inst_group: # group include the instances in machine_id
        #for inst in group['inst_id']:
        # all inst in group with respect to app
        # check disk,P,M,PM
        inst_list = list(group.loc[:, 'inst_id'])
        total_res_in_group = inst_app_df.loc[inst_list, ['disk', 'P', 'M', 'PM']].sum(axis=0)
        if(total_res_in_group['disk']>machine_df.loc[machine_id, 'disk'] or
           total_res_in_group['P']>machine_df.loc[machine_id, 'P'] or
           total_res_in_group['M']>machine_df.loc[machine_id, 'M'] or
           total_res_in_group['PM']>machine_df.loc[machine_id,'PM']):
            #machine_conflict.append(machine_id)
            print("this machine include dpm conficts:", machine_id)

        # check app_interference
        app_group = dict(Counter(list(group.loc[:, 'app_id']))) # each app's number in this machine
        app_list = list(app_group.keys())
        for key1 in app_list:
            for key2 in app_list:
                if (key1, key2) in app_infer_df.index:# check (key1,key2) is in app_infer_df or not
                    if key1 == key2:
                        if app_group[key2] - 1 > app_infer_df.loc[(key1, key2), 'volum']:
                            #machine_conflict.append(machine_id)
                            print((key1, key2), app_infer_df.loc[(key1, key2), 'volum'], '@', machine_id)
                            inst_conflict[machine_id].append(key2)#(list(group.loc[group['app_id'].isin([key2]), 'inst_id']))

                    else:
                        if app_group[key2] > app_infer_df.loc[(key1,key2), 'volum']:
                            #machine_conflict.append(machine_id)
                            print((key1, key2), app_infer_df.loc[(key1, key2), 'volum'], '@', machine_id)
                            inst_conflict[machine_id].append(key2)#(list(group.loc[group['app_id'].isin([key2]), 'inst_id']))

        # check cpu&mem each time slice should satisfy the constraints
        total_cpu_in_group = inst_app_df.loc[list(group.loc[:,'inst_id']), cpu_list].sum(axis=0)
        for cpu in cpu_list:
            if total_cpu_in_group[cpu] > machine_df.loc[machine_id, 'cpu']:
                #machine_conflict.append(machine_id)
                print("this machine violate cpu", machine_id)

        total_mem_in_group = inst_app_df.loc[list(group.loc[:,'inst_id']), mem_list].sum(axis=0)
        for mem in mem_list:
            if total_mem_in_group[mem] > machine_df.loc[machine_id, 'mem']:
                #machine_conflict.append(machine_id)
                print("this machine violate mem", machine_id)

    #machine_conflict = set(machine_conflict)
    for inst in inst_conflict.keys():
        inst_conflict[inst]=set(inst_conflict[inst])

    print('check initial confilct finished!')
    return inst_conflict

def migrateConflictItem(inst_df, inst_conflict, app_infer_df):
    return 0

def isViolate(inst):

def main():

    inst_df,app_resrc_df, machine_df,  app_infer_df = loaddata()

    inst_conflict = checkInitialConflict(inst_df, app_resrc_df, machine_df,app_infer_df)




if __name__=="__main__":
    main()