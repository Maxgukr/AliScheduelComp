"""
process data set
1.find each inst belong to which app
2.find initial state all conflicts conditions, for migrating
"""

from collections import Counter, defaultdict
import pandas as pd
import time
import pickle
import random
#--------------------------------------------------------------------------#

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
        #app_resrc_df.set_index(['app_id'], inplace=False)
        # split cpu&mem
        cpu = app_resrc_df['cpu'].str.split('|', expand=True).astype(float)  # str convert to float
        mem = app_resrc_df['mem'].str.split('|', expand=True).astype(float)  # str convert to float
        cpu_list = ['cpu' + str(i) for i in range(98)]
        mem_list = ['mem' + str(i) for i in range(98)]
        cpu.columns = cpu_list
        mem.columns = mem_list
        # join cpu&mem
        app_resrc_df = app_resrc_df.join(cpu)
        app_resrc_df = app_resrc_df.join(mem)
        del app_resrc_df['cpu']
        del app_resrc_df['mem']
        # merge inst and app
        #inst_app_df = pd.merge(app_resrc_df, inst_df, on=['app_id'], how='left')

        # 按照disk 值排序，由大到小
        #app_resrc_df.sort_values(by="disk", inplace=True, ascending=False)
        #inst_app_df.set_index(['inst_id'], inplace=True)
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
        #machine_df.set_index(['machine_id'], inplace=True)
    finally:
        path_df.close()

    return inst_df, app_resrc_df, machine_df, app_infer_df

def checkConflict(inst_df, inst_app_df, machine_df, appInterference, cpu_list, mem_list):
    """
    # check initial state conflicts, there 4 constraints
    '''
    1. instances' resources(cpu,mem,disk) in machine can't exceed the machine's volume
    2. instances in machine can't violate the app_interference
    3. in same machine, the number of instance of P class, M class, PM class can't exceed the machine's volume
    4. one instance only can be allocated to one machine
    '''
    :param inst_df:
    :param app_resrc_df:
    :param machine_df:
    :param app_infer_df:
    :return:
    """
    # step 1 find vilating app_interference constraint instance for magrating them later
    machine_conflict = []
    app_conflict = defaultdict(list)

    # initial machine_app，machine_inst
    machine_app = defaultdict(list)
    machine_inst = defaultdict(list)

    inst_group = inst_df.groupby(['machine_id'], sort=False) # group instances in same machine
    print("check conflict begin!")
    for machine_id, group in inst_group: # group include the instances in machine_id
        #for inst in group['inst_id']:
        # all inst in group with respect to app

        machine_inst[machine_id] = list(group.loc[:,'inst_id']) # inst list in machine
        machine_app[machine_id] = list(group.loc[:,'app_id']) # app list in machine
        '''
        # check disk,P,M,PM
        total_res_in_group = inst_app_df.loc[list(group.loc[:, 'inst_id']), ['disk', 'P', 'M', 'PM']].sum(axis=0)

        if(total_res_in_group['disk']>machine_df.loc[machine_id, 'disk'] or
           total_res_in_group['P']>machine_df.loc[machine_id, 'P'] or
           total_res_in_group['M']>machine_df.loc[machine_id, 'M'] or
           total_res_in_group['PM']>machine_df.loc[machine_id,'PM']):
            machine_conflict.append(machine_id)
            print("this machine include dpm conficts:", machine_id)
        '''
        # check app_interference
        app_dict = dict(Counter(list(group.loc[:, 'app_id']))) # each app's number in this machine
        #app_list = list(app_dict.keys())

        for key1 in app_dict.keys():
            for key2 in app_dict.keys():
                if (key1, key2) in appInterference.index:# check (key1,key2) is in app_infer_df or not
                    if key1 == key2:
                        if app_dict[key2] - 1 > appInterference[(key1, key2)]:
                            #machine_conflict.append(machine_id)
                            print((key1, key2), appInterference[(key1, key2)], '@', machine_id)

                            app_conflict[machine_id].append(list(group.loc[group['app_id'].isin([key2]), 'inst_id']))
                    else:
                        if app_dict[key2] > appInterference[(key1, key2)]:
                            #machine_conflict.append(machine_id)
                            print((key1, key2), appInterference[(key1, key2)], '@', machine_id)
                            app_conflict[machine_id].append(list(group.loc[group['app_id'].isin([key2]), 'inst_id']))

        if(app_conflict is None):
            print("app_interference passed!")
        '''
        # check cpu&mem each time slice should satisfy the constraints
        if [i for i in list(inst_app_df.loc[list(group.loc[:,'inst_id']), cpu_list].sum(axis=0))
            if i > machine_df.loc[machine_id, 'cpu']]:
            machine_conflict.append(machine_id)
            print("this machine violate cpu", machine_id)

        if [i for i in list(inst_app_df.loc[list(group.loc[:,'inst_id']), mem_list].sum(axis=0))
            if i > machine_df.loc[machine_id, 'mem']]:
            machine_conflict.append(machine_id)
            print("this machine violate mem", machine_id)
            
        
        # check cpu&mem each time slice should satisfy the constraints
        total_cpu_in_group = inst_app_df.loc[list(group.loc[:,'inst_id']), cpu_list].sum(axis=0)
        for cpu in cpu_list:
            if total_cpu_in_group[cpu] > machine_df.loc[machine_id, 'cpu']:
                machine_conflict.append(machine_id)
                print("this machine violate cpu", machine_id)
                #return False

        total_mem_in_group = inst_app_df.loc[list(group.loc[:,'inst_id']), mem_list].sum(axis=0)
        for mem in mem_list:
            if total_mem_in_group[mem] > machine_df.loc[machine_id, 'mem']:
                machine_conflict.append(machine_id)
                print("this machine violate mem", machine_id)
                #return False
        '''
    '''
    if len(machine_conflict)==0:
        print('disk,cpu,mem,p,m,pm all have passed!')
    else:
        machine_conflict = list(set(machine_conflict))
    '''
    if len(app_conflict)==0:
        print("app_interference have passed!")
    else:
        for machineid in app_conflict.keys():
            app_conflict[machineid]=list(set(sum(app_conflict[machineid],[])))

    print('check confilct finished!')

    return app_conflict, machine_inst, machine_app, machine_conflict

def magriteConfilt(app_conflict,
                   cpu_list, mem_list,
                   inst2Appid,
                   appInterference, inst_app_df,
                   machine_app, machine_inst,
                   machineResource, inst_df):
    '''
    magrite conflictted insts in app_conflict
    :param app_conflict: dict:
    :param app_infer_df:
    :param inst_app_df:
    :param machine_app:
    :param machine_df:
    :return:
    '''
    print('magriting conflicted instances beging...')
    result = pd.DataFrame(columns=['inst_id', 'machine_id'])
    #inst_df_c = deepcopy(inst_df)
    machine_list = list(machineResource.keys())
    for machine in app_conflict.keys(): # 处理每个宿主机中有冲突的实例
        for inst in app_conflict[machine]:
            for i in range(len(machine_list)): # 在宿主机列表中寻找第一个适合的主机，并放置该实例

                if isViolate(inst, machine_list[i], cpu_list, mem_list, appInterference, inst_app_df, machine_app,
                         machine_inst, machineResource, inst2Appid)==False:

                    #inst_app_df.replace(inst_app_df.loc[inst, 'machine_id'], machine_list[i], inplace=True)
                    inst_df.loc[inst, 'machine_id'] = machine_list[i]
                    #inst2Machine[inst] = machine
                    result = pd.concat([result, pd.DataFrame({'inst_id': [inst], 'machine_id': [machine_list[i]]})],
                              ignore_index=True)
                    print(inst, machine_list[i])
                    machine_inst[machine].remove(inst)
                    machine_inst[machine_list[i]].append(inst) # update machine_inst

                    machine_app[machine].remove(inst2Appid[inst])
                    machine_app[machine_list[i]].append(inst2Appid[inst])
                    break # 找到一个适合的主机后就跳出当前循环
    print('magriting conflicted instances finished!')

    return result, inst_df #, machine_app, machine_inst

def isViolate(inst_id, machine_id,
              cpu_list, mem_list,
              appInterference, inst_app_df,
              machine_app, machine_inst,
              machineResource, inst2Appid):
    """
    if we put inst_id in machine_id，check whether the inst_id will destroy the constraints in machaine_id
    :param inst_id: the inst will be put
    :param machine_id: the aim machine
    :param inst_df: inst and app map
    :param app_infer_df: include app_interference constrains
    :param inst_app__df: include cpu,mem,disk,p,m,pm constrains
    :param machine_contains: :dict machines has allocated apps, like 'machine_id':[app_id]
    :return: Trur or False
    """
    check_list = []

    if len(machine_app[machine_id])==0 & len(machine_inst[machine_id])==0: # machine_id is empty
        return False
    else:
        # check conflicts
        # get app_id corresponding to inst_id
        app_id = inst2Appid[inst_id] # inst_app_df.loc[inst_id, 'app_id'] # change to dict

        # machine_id current apps
        app_group_temp = machine_app[machine_id] + [app_id]
        app_group = dict(Counter(app_group_temp)) # statistic each app class number

        # check app_inteference
        for app1 in app_group.keys():
            for app2 in app_group.keys():
                if (app1, app2) in appInterference.keys(): # change to dict

                    if app1==app2:
                        if appInterference[(app1, app2)] < app_group[app2]-1: # change to dict
                            #return True
                            check_list.append(True)
                        else:
                            #return False
                            check_list.append(False)
                    else:
                        if appInterference[(app1, app2)] < app_group[app2]:
                            #return True
                            check_list.append(True)
                        else:
                            #return False
                            check_list.append(False)

        # check disk, p,m,pm
        # compute rest resource in machine_id

        app_temp = machine_inst[machine_id] + [inst_id] # add a new inst
        # after putting inst_id, the total resource

        total_res_in_group = inst_app_df.loc[app_temp, ['disk', 'P']].sum(axis=0)

        if (total_res_in_group['disk'] > machineResource[machine_id]['disk'] or # change to dict
            total_res_in_group['P'] > machineResource[machine_id]['P']):
            #total_res_in_group['M'] > machineResource[machine_id]['M'] or
            #total_res_in_group['PM'] > machineResource[machine_id]['PM']):
            check_list.append(True)
        else:
            check_list.append(False)

        # check cpu&mem
        # check cpu&mem each time slice should satisfy the constraints
        if len([i for i in list(inst_app_df.loc[app_temp, cpu_list].sum(axis=0))
            if i > machineResource[machine_id]['cpu']])!=0:
            check_list.append(True)
        else:
            check_list.append(False)
        if len([i for i in list(inst_app_df.loc[app_temp, mem_list].sum(axis=0))
            if i > machineResource[machine_id]['mem']]):
            check_list.append(True)
        else:
            check_list.append(False)

    del app_group_temp
    del app_group
    del app_temp, app_id
    del total_res_in_group

    if True in check_list:
        return True
    else:
        return False
#-------------------------------------------------------------------------#
def machineContain(machine_df, inst_df):

    # 计算每个machine的资源使用量（现在有哪些inst部署在其中）
    inst_machine_df = pd.merge(machine_df, inst_df, on=['machine_id'], how='left')
    # inst_machine_df.fillna(0)
    # inst_machine_df.set_index(['machine_id'], inplace=True)
    machine_Used = inst_machine_df[['machine_id', 'inst_id']].groupby(['machine_id'], sort=False)
    machine_used = {}
    for machine, group in machine_Used:
        machine_used[machine] = list(group.loc[:, 'inst_id'])

    fw5 = open('./data/machine_used.txt', 'wb')
    pickle.dump(machine_used, fw5)

    return  machine_used

def calMachinesRest(machine_used, inst_app_df, cpu_list, mem_list):
    '''
    计算迁移之后分配了实例的机器的余量
    :param machineUsed:
    :return:
    '''
    cpu1 = [32 for i in range(98)]
    mem1 = [64 for i in range(98)]

    cpu2 = [92 for i in range(98)]
    mem2 = [288 for i in range(98)]

    machine_flavor = {1: cpu1+mem1+[600, 7], 2: cpu2+mem2+[1024, 7]}
    machineRest = {}

    for machine in list(machine_used.keys()):
        machine_num = int(machine.split('_')[1])
        if machine_num > 3000:
            if type(machine_used[machine][0])==float:
                machineRest[machine] = machine_flavor[2]
            else:
                cpu =list(inst_app_df.loc[machine_used[machine], cpu_list].sum(axis=0))
                mem = list(inst_app_df.loc[machine_used[machine], mem_list].sum(axis=0))
                resrc = cpu + mem + list(inst_app_df.loc[machine_used[machine], ['disk', 'P']].sum(axis=0))
                machineRest[machine] = list(map(lambda x:x[0] - x[1], zip(machine_flavor[2], resrc)))

        else:
            if  type(machine_used[machine][0])==float:
                machineRest[machine] = machine_flavor[1]
            else:
                cpu = list(inst_app_df.loc[machine_used[machine], cpu_list].sum(axis=0))
                mem = list(inst_app_df.loc[machine_used[machine], mem_list].sum(axis=0))
                resrc = cpu + mem + list(inst_app_df.loc[machine_used[machine], ['disk', 'P']].sum(axis=0))
                machineRest[machine] = list(map(lambda x:x[0] - x[1], zip(machine_flavor[1], resrc)))

    del machine_flavor
    del cpu1,cpu2,mem1,mem2

    rest = list(machineRest.keys())
    for m in rest:
        if machineRest[m][196] < 40: #去掉无法放置的machine
            del machineRest[m]

    fw4 = open('./data/machineRest.txt', 'wb')
    pickle.dump(machineRest, fw4)

    return machineRest

def calOneMachineRest(inst_id, machine_id, machine_rest, inst_app_df, cpu_list, mem_list):


    rest = list(map(lambda x:x[0]-x[1],
                    zip(machine_rest[machine_id], inst_app_df.loc[inst_id, cpu_list+mem_list+['disk', 'P']])))
    if len([i for i in rest if i <0])!=0:
        return machine_rest[machine_id], False
    else:
        return rest, True

def checkAppInter(inst_id, machine_id, appInterference, machine_app, inst2Appid):
    # get app_id corresponding to inst_id\
    check_list = []
    app_id = inst2Appid[inst_id]  # inst_app_df.loc[inst_id, 'app_id'] # change to dict

    # machine_id current apps
    app_group_temp = machine_app[machine_id] + [app_id]
    app_group = dict(Counter(app_group_temp))  # statistic each app class number

    # check app_inteference
    for app1 in app_group.keys():
        for app2 in app_group.keys():
            if (app1, app2) in appInterference.keys():  # change to dict

                if app1 == app2:
                    if appInterference[(app1, app2)] < app_group[app2] - 1:  # change to dict
                        # return True
                        check_list.append(False)
                    else:
                        # return False
                        check_list.append(True)
                else:
                    if appInterference[(app1, app2)] < app_group[app2]:
                        # return True
                        check_list.append(False)
                    else:
                        # return False
                        check_list.append(True)

    if False in check_list:
        return False
    else:
        return True

def classifyInstByDisk(inst_to_alloc, inst2disk):
    '''
    将待放置的实例集合按照disk的需求进行分类
    disk相同的实例，按照cpu的均值分类
    :param inst_set:
    :return:
    '''
    inst_disk = defaultdict(list)
    for inst in inst_to_alloc:
        if inst2disk[inst] == 1024:
            inst_disk[1024].append(inst)
        elif inst2disk[inst] == 1000:
            inst_disk[1000].append(inst)
        elif inst2disk[inst] == 650:
            inst_disk[650].append(inst)
        elif inst2disk[inst] == 600:
            inst_disk[600].append(inst)
        elif inst2disk[inst] == 500:
            inst_disk[500].append(inst)
        elif inst2disk[inst] == 300:
            inst_disk[300].append(inst)
        elif inst2disk[inst] == 250:
            inst_disk[250].append(inst)
        elif inst2disk[inst] == 200:
            inst_disk[200].append(inst)
        elif inst2disk[inst] == 180:
            inst_disk[180].append(inst)
        elif inst2disk[inst] == 167:
            inst_disk[167].append(inst)
        elif inst2disk[inst] == 150:
            inst_disk[150].append(inst)
        elif inst2disk[inst] == 120:
            inst_disk[120].append(inst)
        elif inst2disk[inst] == 100:
            inst_disk[100].append(inst)
        elif inst2disk[inst] == 80:
            inst_disk[80].append(inst)
        elif inst2disk[inst] == 60:
            inst_disk[60].append(inst)
        elif inst2disk[inst] == 40:
            inst_disk[40].append(inst)

    return inst_disk

def magriteF1_to_F2(machine_used, machine_rest_not_null, machine_rest_null,
                    machine_app,cpu_list, mem_list, inst2Appid,
                    appInterference, res, inst_df, inst_app_df):


    '''

    :param machine_used:
    :param machine_rest_not_null:
    :param machine_rest_null:
    :param inst2dict:
    :return:
    '''
    cpu1 = [32 for i in range(98)]
    mem1 = [64 for i in range(98)]

    cpu2 = [92 for i in range(98)]
    mem2 = [288 for i in range(98)]

    machine_flavor = {1: cpu1 + mem1 + [600, 7], 2: cpu2 + mem2 + [1024, 7]}

    #把剩余的机器分成两类
    machine_rest_null_0 = []
    machine_rest_null_3000 = []
    machine_rest_not_null_3000 = []
    machine_rest_not_null_0 = []
    for m in list(machine_rest_not_null.keys()):
        num = int(m.split('_')[1])
        if num > 3000:
            machine_rest_not_null_3000.append(m)
        else:
            machine_rest_not_null_0.append(m)

    for n in list(machine_rest_null.keys()):
        num = int(n.split('_')[1])
        if num > 3000:
            machine_rest_null_3000.append(n)
        else:
            machine_rest_null_0.append(n)

    cnt = 0
    cnt_m = 0

    # 把规格1中的机器全部转移出来放到规格2 的机器中
    machine_to_magrite = []
    for p in machine_rest_not_null_0:
        machine_to_magrite += machine_used[p]

        for j in machine_used[p]:
            machine_app[p].remove(inst2Appid[j])  # 约束也要更新

        machine_used[p] = []
        machine_rest_null[p] = machine_flavor[1]
        del machine_rest_not_null[p] # 删掉非空剩余机器中规格为 1 的机器
        machine_rest_null_0.append(p)

    #迁移到空的规格为2 的机器中
    for m_d in machine_rest_null_3000:
        for inst in machine_to_magrite:
            rest_tmp, logitic = calOneMachineRest(inst, m_d, machine_rest_null, inst_app_df, cpu_list, mem_list)
            # 判断nachine剩余资源状态
            '''
            if checkCPU(rest_tmp, num) == False:  # 判断cpu余量
                break
    
            if checkMEM(machine_rest_not_null[m], num) == False:
                break
            '''
            if logitic == True:  # 可以容下

                if checkAppInter(inst, m_d, appInterference, machine_app, inst2Appid):  # 满足app约束

                    res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m_d]})],ignore_index=True)  # 记录结果
                    inst_df.loc[inst, 'machine_id'] = m_d  # 更新文件


                    machine_app[m_d].append(inst2Appid[inst]) # 增加

                    machine_rest_null[m_d] = rest_tmp

                    #machine_used[m].remove(inst)
                    machine_used[m_d].append(inst)
                    machine_to_magrite.remove(inst)
                    cnt += 1
                    print(inst, m_d, cnt, cnt_m)
                    # break # 放完一个inst就退出该层循环

                    if rest_tmp[196] < 40 :  # 判断disk 是否够用
                        print(rest_tmp[196])
                        cnt_m += 1
                        del machine_rest_null[m_d] #放满了就删掉
                        machine_rest_null_3000.remove(m_d)
                        #machine_rest_not_null_3000.append(m_d)
                        #m_d = random.sample(machine_rest_null_3000, 1)[0]  # 随机取样
                        break
    print('magrite over!')
    return machine_rest_not_null_3000, machine_rest_null_0, machine_rest_null_3000, res

def alloc_120_1024(inst_disk, machine_rest_null_3000,
                   machine_rest_null, inst_app_df,
                   cpu_list, mem_list, inst_df,
                   appInterference, machine_app,
                   inst_to_alloc, inst2Appid, res):
    '''
    先把disk规模在120-1024disk先放完
    剩余资源与数量的关系为：
    [1024,6],[650,3],[600,12],[500,35],[300,159],[250,12],[200,663],[180,13],[167,38],[150,191],[120,64]
    :param inst_disk: 每种disk包含的inst
    :param machine_rest_null_3000:规格为2的宿主机集合
    :return:
    '''

    # 从大到小开始组合放置
    cnt = 0
    for m in machine_rest_null_3000:
        cnt += 1
        if len(inst_disk[1024]) != 0:
            for inst in inst_disk[1024]:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_disk[1024].remove(inst)
                        inst_to_alloc.remove(inst)

                        print(inst, m, cnt)
                        #break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[650]) != 0:
            inst_list = inst_disk[650] + inst_disk[250] + inst_disk[120]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[650]:
                            inst_disk[650].remove(inst)
                        elif inst in inst_disk[250]:
                            inst_disk[250].remove(inst)
                        elif inst in inst_disk[120]:
                            inst_disk[120].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40 :  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[600]) != 0:
            inst_list = inst_disk[600] + inst_disk[300] + inst_disk[120]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[600]:
                            inst_disk[600].remove(inst)
                        elif inst in inst_disk[300]:
                            inst_disk[300].remove(inst)
                        elif inst in inst_disk[120]:
                            inst_disk[120].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[500]) != 0:
            inst_list = inst_disk[500] + inst_disk[300] + inst_disk[200]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[500]:
                            inst_disk[500].remove(inst)
                        elif inst in inst_disk[300]:
                            inst_disk[300].remove(inst)
                        elif inst in inst_disk[200]:
                            inst_disk[200].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[300]) != 0:
            inst_list = inst_disk[300] + inst_disk[200] + inst_disk[120] + inst_disk[100]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[300]:
                            inst_disk[300].remove(inst)
                        elif inst in inst_disk[200]:
                            inst_disk[200].remove(inst)
                        elif inst in inst_disk[120]:
                            inst_disk[120].remove(inst)
                        elif inst in inst_disk[100]:
                            inst_disk[100].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[200]) != 0:
            #inst_list = inst_disk[200]
            for inst in inst_disk[200]:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_disk[200].remove(inst)
                        print(inst, m, cnt)

                    #if inst in inst_disk[200]:
                        #inst_disk[200].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[180]) != 0:
            inst_list = inst_disk[180] + inst_disk[150] + inst_disk[60]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)
                        if inst in inst_disk[180]:
                            inst_disk[180].remove(inst)
                        elif inst in inst_disk[150]:
                            inst_disk[150].remove(inst)
                        elif inst in inst_disk[60]:
                            inst_disk[60].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[167]) != 0:
            inst_list = inst_disk[167] + inst_disk[150] + inst_disk[80]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[167]:
                            inst_disk[167].remove(inst)
                        elif inst in inst_disk[150]:
                            inst_disk[150].remove(inst)
                        elif inst in inst_disk[80]:
                            inst_disk[80].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[150]) != 0:
            inst_list = inst_disk[150] + inst_disk[60]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[150]:
                            inst_disk[150].remove(inst)
                        elif inst in inst_disk[60]:
                            inst_disk[60].remove(inst)
                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[250]) != 0:
            inst_list = inst_disk[250] + inst_disk[80] + inst_disk[40]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[250]:
                            inst_disk[250].remove(inst)
                        elif inst in inst_disk[80]:
                            inst_disk[80].remove(inst)
                        elif inst in inst_disk[40]:
                            inst_disk[40].remove(inst)
                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        elif len(inst_disk[120]) != 0:
            inst_list = inst_disk[120] + inst_disk[100] + inst_disk[40]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[120]:
                            inst_disk[120].remove(inst)
                        elif inst in inst_disk[100]:
                            inst_disk[100].remove(inst)
                        elif inst in inst_disk[40]:
                            inst_disk[40].remove(inst)
                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            break
        else:
            break

    return res, inst_df

def alloc_40_100(inst_disk,
                   machine_rest_null, inst_app_df,
                   cpu_list, mem_list, inst_df,
                   appInterference, machine_app, inst2Appid, res):
    '''
    :param inst_disk:
    :param machine_rest_null:
    :param inst_app_df:
    :param cpu_list:
    :param mem_list:
    :param appInterference:
    :param machine_app:
    :param inst2Appid:
    :param res:
    :return:
    '''
    inst_list = inst_disk[60] + inst_disk[40] + inst_disk[100] + inst_disk[80]
    machine_list = list(machine_rest_null.keys())
    random.shuffle(inst_list)
    while len(inst_list)!=0:
        for inst in inst_list:

            for m in machine_list:
                rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
                '''
                if rest_tmp[196] < 40:  # 判断disk 是否够用
                    print(rest_tmp[196], )
                    #del machine_rest_null[m]
                    machine_list.remove(m)
                    break'''
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下

                    if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                        res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                        ignore_index=True)  # 记录结果
                        inst_df.loc[inst, 'machine_id'] = m # 更新文件
                        machine_app[m].append(inst2Appid[inst])
                        machine_rest_null[m] = rest_tmp

                        inst_list.remove(inst)

                        print(inst, m, len(inst_list), len(machine_list))
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            del machine_rest_null[m]
                            machine_list.remove(m)
                        break # 放完一个inst就退出该层循环

    return res, inst_df

def initData(inst_app_df, inst_df, machine_df, app_infer_df):
    '''
    计算每个inst对应的资源和每个machine对应的剩余资源
    将appInterference 存储为2维numpy数组
    将instResource 和 machineRestResource 也存为numpy二维数组
    :param inst_app_df:
    :return:
    '''
    instResource = {}
    machineRest = {}
def main():


    cpu_list = ['cpu' + str(i) for i in range(98)]
    mem_list = ['mem' + str(i) for i in range(98)]

    _, app_resrc_df, machine_df,  app_infer_df = loaddata()


    # 迁移不会改变需要放置的实例的数量和类别

    '''
    app_conflict, machine_inst, machine_app, machine_conflict = checkConflict(inst_df, inst_app_df,
                                                                              machine_df,app_infer_df,
                                                                                                                                                      cpu_list, mem_list)

    
    
    # 更改已放置实例对应的宿主机
    res, inst_df = magriteConfilt(app_conflict,
                       cpu_list, mem_list,
                       inst2Appid,
                       appInterference, inst_app_df,
                       machine_app, machine_inst,
                       machineResource, inst_df)

    inst_df.to_csv('./data/inst_df.csv', header=False, index=False, chunksize=10000)
    
    #res = pd.DataFrame(res)
    #res.to_csv('./data/res.csv', header=False, index=False)

    #
    
    fw1 = open('./data/machine_inst.txt', 'wb')
    fw2 = open('./data/machine_app.txt', 'wb')
    fw3 = open('./data/res.txt', 'wb')
    pickle.dump(res,fw3)
    pickle.dump(machine_inst, fw1)
    pickle.dump(machine_app, fw2)
    
    #inst_df = pd.read_csv('./data/inst_df.csv', index_col=False, names=[ 'inst_id', 'app_id', 'machine_id'])

    

    # magriting process result
    res = pd.read_csv('./data/res.csv', index_col=False, header=None, names=['inst_id','machine_id'])
    #fr1 = open('./data/machine_inst.txt', 'rb')
    fr2 = open('./data/machine_app.txt', 'rb')
    fr3 = open('./data/machine_used.txt','rb')
    fr4 = open('./data/machine_rest_not_null.txt','rb')
    fr5 = open('./data/machine_rest_null.txt','rb')
    fr6 = open('./data/machineRest_all.txt','rb')
    #machine_inst = pickle.load(fr1)
    machine_app = pickle.load(fr2)
    machine_uesd = pickle.load(fr3)
    #machine_rest_not_null = pickle.load(fr4) # machine的资源剩余量
    #machine_rest_null = pickle.load(fr5)
    #machineRest = pickle.load(fr6)

    '''
    fr2 = open('./data/machine_app.txt', 'rb')
    machine_app = pickle.load(fr2)
    res = pd.read_csv('./data/res.csv', index_col=False, header=None, names=['inst_id', 'machine_id'])
    inst_df = pd.read_csv('./data/inst_df.csv',index_col=False,names=['inst_id','app_id','machine_id'])
    inst_app_df = pd.merge(app_resrc_df, inst_df, on=['app_id'], how='left')
    inst_app_df.set_index(['inst_id'], inplace=True)
    # save info as dict
    appInterference = app_infer_df['volum'].to_dict()
    inst2disk = inst_app_df['disk'].to_dict()

    # 计算每个machine包含的实例
    fr3 = open('./data/machine_used.txt', 'rb')
    machine_uesd = pickle.load(fr3)
    #machine_used = machineContain(machine_df, inst_df)

    inst_df.set_index(['inst_id'], inplace=True)
    inst2Appid = inst_df['app_id'].to_dict()
    inst2Machine = inst_df['machine_id'].to_dict()
    # 计算每个machine的剩余资源
    #machineRest = calMachinesRest(machine_uesd, inst_app_df, cpu_list, mem_list)
    fr6 = open('./data/machineRest.txt', 'rb')
    machineRest = pickle.load(fr6)

    #fr = open('./data/machineRest_all.txt')
    # 将剩余资源分类，资源全满和不全满
    machine_rest_null = {}
    machine_rest_not_null = {}

    for m in machineRest.keys():
        if machineRest[m][0] != 32 and machineRest[m][0] != 92:
            machine_rest_not_null[m] = machineRest[m]
        else:
            machine_rest_null[m] = machineRest[m]

    # 剩下的需要放置的实例
    inst_to_alloc = list(inst_app_df.index[inst_df['machine_id'].isnull().values == True])
    inst_to_alloc.reverse()
    #将disk未放满的非空宿主机（201个）按规格分类，并将【32,64,600】的虚拟机迁移到【92,288,1024】中
    machine_rest_not_null_3000, \
    machine_rest_null_0, \
    machine_rest_null_3000, res =  magriteF1_to_F2(machine_uesd,
                                              machine_rest_not_null,
                                              machine_rest_null,
                                              machine_app,
                                              cpu_list,
                                              mem_list,
                                              inst2Appid,
                                              appInterference, res, inst_df, inst_app_df)


    print('begining allocating the rest instances...')
    cnt = 0
    # 首先把非空的规格为【92,288,1024】机器先放完
    for m in machine_rest_not_null_3000:
        cnt += 1
        for inst in inst_to_alloc:
            rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_not_null, inst_app_df, cpu_list, mem_list)
            # 判断nachine剩余资源状态
            if logitic == True:  # 可以容下
                if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                    res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],ignore_index=True) # 记录结果
                    inst_df.loc[inst, 'machine_id'] = m # 更新文件
                    machine_app[m].append(inst2Appid[inst])
                    machine_rest_not_null[m] = rest_tmp

                    inst_to_alloc.remove(inst)
                    print(inst, m)
                    #break # 放完一个inst就退出该层循环
                    if rest_tmp[196] < 40:  # 判断disk 是否够用
                        print(rest_tmp[196], cnt)
                        del machine_rest_not_null[m]
                        break

    # 接下来放machine_rest_null中的机器，先放置规格为2的，以及组合放置disk:120-1024的inst

    # 先将inst按照disk分类
    inst_disk = classifyInstByDisk(inst_to_alloc, inst2disk)

    # 将分好类的disk组合优化
    # 先放120-1024 的实例
    res, inst_df = alloc_120_1024(inst_disk, machine_rest_null_3000,
                   machine_rest_null, inst_app_df,
                   cpu_list, mem_list, inst_df,
                   appInterference, machine_app, inst2Appid, res)
    # 再放40-100的实例
    res, inst_df = alloc_40_100(inst_disk,
                 machine_rest_null, inst_app_df,
                 cpu_list, mem_list, inst_df,
                 appInterference, machine_app, inst2Appid, res)


    '''
    fw3 = open('./data/inst_disk.txt','wb')
    pickle.dump(inst_disk, fw3)

    
    res.to_csv('./data/res.csv', header=False, index=False)
    fw2 = open('./data/machine_app.txt', 'wb')
    pickle.dump(machine_app, fw2)
    inst_df.to_csv('./data/inst_df.csv', header=False, index=True, chunksize=10000)
    '''

    rest_inst = list(inst_df.index[inst_df['machine_id'].isnull().values == True])
    if(len(rest_inst))!=0:
        print("still have rest insts!")

    machine_list = list(machine_rest_null.keys())
    for inst in rest_inst:
        for m in machine_list:
            rest_tmp, logitic = calOneMachineRest(inst, m, machine_rest_null, inst_app_df, cpu_list, mem_list)
            '''
            if rest_tmp[196] < 40:  # 判断disk 是否够用
                print(rest_tmp[196], )
                #del machine_rest_null[m]
                machine_list.remove(m)
                break'''
            # 判断nachine剩余资源状态
            if logitic == True:  # 可以容下

                if checkAppInter(inst, m, appInterference, machine_app, inst2Appid):  # 满足app约束

                    res = pd.concat([res, pd.DataFrame({'inst_id': [inst], 'machine_id': [m]})],
                                    ignore_index=True)  # 记录结果
                    inst_df.loc[inst, 'machine_id'] = m  # 更新文件
                    machine_app[m].append(inst2Appid[inst])
                    machine_rest_null[m] = rest_tmp

                    print(inst, m, len(rest_inst), len(machine_list))
                    if rest_tmp[196] < 40:  # 判断disk 是否够用
                        cnt += 1
                        print(rest_tmp[196])
                        del machine_rest_null[m]
                        machine_list.remove(m)
                    break  # 放完一个inst就退出该层循环


    print('finished allocating the rest instances')

    res.to_csv('./data/submit.csv', header=False, index=False)
    inst_df.to_csv('./data/inst_df.csv', header=False, index=False, chunksize=10000)

    print('check the constraints')
    _1, _2, _3, _4 = checkConflict(inst_df, inst_app_df, machine_df, app_infer_df, cpu_list, mem_list)

    if len(_1)==1 & len(_4) == 0 & ('nan' not in list(inst_df['machine_id'])):
        print('allocated all insatnces and satisfied all constrains!')



if __name__=="__main__":
    main()