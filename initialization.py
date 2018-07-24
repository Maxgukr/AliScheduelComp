from collections import Counter, defaultdict
import time
import pickle
import random
import numpy as np

def loaddata():

    inst_path = "./data/instance_deploy_20180606.csv"
    machine_path = "./data/machine_resources_20180606.csv"
    app_resrc_path = "./data/app_resources_20180606.csv"
    app_infer_path = "./data/app_interference_20180606.csv"

    inst2Appid = {} # map inst to appid
    inst2Machine = {} # map inst to machine_id
    appResource = [] # each app rescource
    appInterference = np.ones((9338,9338), dtype=int)*10 # app inter constrains
    machineResource = [] # machine resource now
    machineApps = defaultdict(list) # app num in each machine
    machineInsts = defaultdict(list)

    # load data

    with open(machine_path, 'r') as f:
        for line in f:
            line_list = line.strip('\n').split(',')
            machineResource.append([int(line_list[1]) for i in range(98)]+
                                   [int(line_list[2]) for i in range(98)]+
                                   [int(line_list[3]), int(line_list[4])])
    machineResource = np.array(machineResource, dtype=float)
    f.close()

    with open(app_resrc_path, 'r') as f:
        for line in f:
            line_list = line.strip('\n').split(',')
            cpu = line_list[1].split('|')
            mem = line_list[2].split('|')
            appResource.append(cpu + mem + [line_list[3], line_list[4]])
    appResource = np.array(appResource, dtype=float)

    with open(inst_path,'r') as f:
        for line in f:
            line_list = line.strip('\n').split(',')
            inst_num = int(line_list[0].split('_')[1])
            app_num = int(line_list[1].split('_')[1])
            inst2Appid[inst_num] = app_num
            if line_list[2] != '':
                machine_num = int(line_list[2].split('_')[1])
                inst2Machine[inst_num] = machine_num
                machineApps[machine_num].append(app_num)
                machineInsts[machine_num].append(inst_num)
                machineResource[machine_num-1] -= appResource[app_num-1]
            else:
                inst2Machine[inst_num] = []
    f.close()

    with open(app_infer_path,'r') as f:
        for line in f:
            line_list = line.strip('\n').split(',')
            app1 = int(line_list[0].split('_')[1])
            app2 = int(line_list[1].split('_')[1])
            appInterference[app1-1][app2-1] = int(line_list[2])

    return inst2Appid, inst2Machine,  machineApps, machineInsts, appResource, appInterference, machineResource

def write2file(res):
    path = './result.csv'
    with open(path,'w') as f:
        for line in res:
            f.write(line+'\n')

def checkConflict(appInterference, machineApps, machineInsts, inst2App):

    print('checking app_interference conflict...')
    machine_conflicts = defaultdict(list)
    for machine_num in machineInsts.keys():
        # machine_num current apps
        app_group_temp = machineApps[machine_num]
        app_group = dict(Counter(app_group_temp))  # statistic each app class number

        # each app include insts
        appInst = defaultdict(list)
        for app in app_group.keys():
            for inst in machineInsts[machine_num]:
                if inst2App[inst] == app: # inst belong app
                    appInst[app].append(inst)

        # check app_inteference
        for app1 in app_group.keys():
            for app2 in app_group.keys():
                if app1 == app2:
                    if appInterference[app1-1][app2-1] < app_group[app2] - 1:
                        #machine_conflicts.append(machine_num) # 有冲突的机器序号
                        machine_conflicts[machine_num].append(appInst[app2])
                        print(app1,app2, machine_num)
                else:
                    if appInterference[app1-1][app2-1] < app_group[app2]:
                        #machine_conflicts.append(machine_num)
                        machine_conflicts[machine_num].append(appInst[app2])
                        print(app1, app2, machine_num)
    #machine_conflicts = list(set(machine_conflicts))
    for machineid in machine_conflicts.keys():
        machine_conflicts[machineid] = list(set(sum(machine_conflicts[machineid], [])))
    print('checking over!')
    return machine_conflicts

def magrite(machine_conflict, machineApps, machineInsts,
                  inst2Machine, machineResource, appResource,
                  inst2App, appInterference):

    print('magriting conflicted instances beging...')
    res=[]
    machine_list = [i+1 for i in range(3000)] #1-6000
    cnt = 0
    for m in machine_list:  # 在宿主机列表中寻找第一个适合的主机，并放置该实例
        for m_c in machine_conflict.keys(): # m_c : machine_num
            for inst in machine_conflict[m_c]:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App) # 记得序号要减1
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m

                        res.append('inst_'+str(inst)+','+'machine_'+str(m)) # write magrite processment

                        machineApps[m_c].remove(inst2App[inst])
                        machineApps[m].append(inst2App[inst])

                        machineInsts[m_c].remove(inst)
                        machineInsts[m].append(inst)

                        machine_conflict[m_c].remove(inst)

                        cnt += 1
                        machineResource[m_c-1] += appResource[inst2App[inst]-1] # 序号到数组需要-1
                        machineResource[m-1] = rest_tmp
                        print(inst, m)
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            #del machineResource[m]
                            machine_list.remove(m)
                            break # 放完一个inst就退出该层循环
    print('magriting over!')
    return res

def calOneMachineRest(inst_id, machine_id, machineRescouce, appResource, inst2App):

    rest = machineRescouce[machine_id] - appResource[inst2App[inst_id]-1] # 序号到数组需要-1

    if len([i for i in rest if i <0])!=0 : # 资源超分或者cpu利用率太高
        return machineRescouce[machine_id], False
    else:
        return rest, True

def checkAppInter(inst_id, machine_id, appInterference, machineApps, inst2App):
    # get app_id corresponding to inst_id\
    check_list = []
    app_id = inst2App[inst_id]  # inst_app_df.loc[inst_id, 'app_id'] # change to dict

    # machine_id current apps
    app_group_temp = machineApps[machine_id] + [app_id]
    app_group = dict(Counter(app_group_temp))  # statistic each app class number

    # check app_inteference
    for app1 in app_group.keys():
        for app2 in app_group.keys():
            if app1 == app2:
                if appInterference[app1-1][app2-1] < app_group[app2] - 1:
                    check_list.append(False)
                else:
                    check_list.append(True)
            else:
                if appInterference[app1-1][app2-1] < app_group[app2]:
                    check_list.append(False)
                else:
                    check_list.append(True)

    if False in check_list:
        return False
    else:
        return True

def classifyInstByDisk(inst_to_alloc, inst2App, appResource):
    '''
    将待放置的实例集合按照disk的需求进行分类
    disk相同的实例，按照cpu的均值分类
    :param inst_set:
    :return:
    '''
    inst_disk = defaultdict(list)
    for inst in inst_to_alloc:
        if appResource[inst2App[inst]-1][196] == 1024:
            inst_disk[1024].append(inst)
        elif appResource[inst2App[inst]-1][196] == 1000:
            inst_disk[1000].append(inst)
        elif appResource[inst2App[inst]-1][196] == 650:
            inst_disk[650].append(inst)
        elif appResource[inst2App[inst]-1][196] == 600:
            inst_disk[600].append(inst)
        elif appResource[inst2App[inst]-1][196] == 500:
            inst_disk[500].append(inst)
        elif appResource[inst2App[inst]-1][196] == 300:
            inst_disk[300].append(inst)
        elif appResource[inst2App[inst]-1][196] == 250:
            inst_disk[250].append(inst)
        elif appResource[inst2App[inst]-1][196] == 200:
            inst_disk[200].append(inst)
        elif appResource[inst2App[inst]-1][196] == 180:
            inst_disk[180].append(inst)
        elif appResource[inst2App[inst]-1][196] == 167:
            inst_disk[167].append(inst)
        elif appResource[inst2App[inst]-1][196] == 150:
            inst_disk[150].append(inst)
        elif appResource[inst2App[inst]-1][196] == 120:
            inst_disk[120].append(inst)
        elif appResource[inst2App[inst]-1][196] == 100:
            inst_disk[100].append(inst)
        elif appResource[inst2App[inst]-1][196] == 80:
            inst_disk[80].append(inst)
        elif appResource[inst2App[inst]-1][196] == 60:
            inst_disk[60].append(inst)
        elif appResource[inst2App[inst]-1][196] == 40:
            inst_disk[40].append(inst)

    return inst_disk

def claasifyMachine(machineResource):

    machine_rest_not_null_3000 = []
    machine_rest_not_null_0 = []
    machine_rest_null_3000 = []
    machine_rest_null_0 = []
    for m in range(6000):
        if machineResource[m][196] >= 40:  # 机器中disk还有剩余空间可以放的
            if abs(machineResource[m][0] - 32.0) > 0.001 and abs(machineResource[m][0] - 92.0) > 0.001:
                if m + 1 > 3000:
                    machine_rest_not_null_3000.append(m + 1)  # 记录机器序号+1
                else:
                    machine_rest_not_null_0.append(m + 1)
            else:
                if m + 1 > 3000:
                    machine_rest_null_3000.append(m + 1)  # 记录机器序号+1
                else:
                    machine_rest_null_0.append(m + 1)

    return  machine_rest_not_null_3000,machine_rest_not_null_0, machine_rest_null_3000, machine_rest_null_0

def underAllocInst(inst2Mchine):
    inst_to_alloc = [] # 迁移不会改变未分配的实例的数量，这是迁移还剩下的实例数量
    for inst in inst2Mchine.keys():
        if type(inst2Mchine[inst]) == list:
            inst_to_alloc.append(inst)

    return inst_to_alloc

def magriteF1_F2(machine_rest_null_0, machine_rest_not_null_0,
                 machineApps, machineInsts,
                  inst2Machine, machineResource, appResource,
                  inst2App, appInterference, res):

    print('magriting instances beging...')
    machine_list = [i+3001 for i in range(3000)] #3001-6000
    cnt = 0
    for m in machine_list:  # 在宿主机列表中寻找第一个适合的主机，并放置该实例
        for m_c in machine_rest_not_null_0: # m_c : machine_num
            for inst in machineInsts[m_c]:

                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App) # 记得序号要减1
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m

                        res.append('inst_'+str(inst)+','+'machine_'+str(m)) # write magrite processment

                        machineApps[m_c].remove(inst2App[inst])
                        machineApps[m].append(inst2App[inst])

                        machineInsts[m_c].remove(inst)
                        machineInsts[m].append(inst)

                        #machine_conflict[m_c].remove(inst)
                        if len(machineInsts[m_c]) == 0:
                            machine_rest_not_null_0.remove(m_c)
                            machine_rest_null_0.append(m_c)
                            cnt += 1

                        machineResource[m_c-1] += appResource[inst2App[inst]-1] # 序号到数组需要-1
                        machineResource[m-1] = rest_tmp
                        print(inst, m, cnt)
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            machine_list.remove(m)
                            break # 放完一个inst就退出该层循环
    print('magriting over!')

def alloc_120_1024(inst_disk, machine_rest_null_3000,
                   machineApps, machineResource, appResource,
                   appInterference, inst2App, res, inst2Machine):
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
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))

                        machineApps[m].append(inst2App[inst])
                        machineResource[m-1] = rest_tmp

                        inst_disk[1024].remove(inst)
                        #inst_to_alloc.remove(inst)

                        print(inst, m, cnt)
                        #break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[650]) != 0:
            inst_list = inst_disk[650] + inst_disk[250] + inst_disk[120]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[600]) != 0:
            inst_list = inst_disk[600] + inst_disk[300] + inst_disk[120]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m)) # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[500]) != 0:
            inst_list = inst_disk[500] + inst_disk[300] + inst_disk[200]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[300]) != 0:
            inst_list = inst_disk[300] + inst_disk[200] + inst_disk[120] + inst_disk[100]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[200]) != 0:
            #inst_list = inst_disk[200]
            for inst in inst_disk[200]:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

                        inst_disk[200].remove(inst)
                        print(inst, m, cnt)

                    #if inst in inst_disk[200]:
                        #inst_disk[200].remove(inst)

                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[180]) != 0:
            inst_list = inst_disk[180] + inst_disk[150] + inst_disk[60]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[167]) != 0:
            inst_list = inst_disk[167] + inst_disk[150] + inst_disk[80]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[150]) != 0:
            inst_list = inst_disk[150] + inst_disk[60]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

                        inst_list.remove(inst)
                        print(inst, m, cnt)

                        if inst in inst_disk[150]:
                            inst_disk[150].remove(inst)
                        elif inst in inst_disk[60]:
                            inst_disk[60].remove(inst)
                        # break # 放完一个inst就退出该层循环
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[250]) != 0:
            inst_list = inst_disk[250] + inst_disk[80] + inst_disk[40]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        elif len(inst_disk[120]) != 0:
            inst_list = inst_disk[120] + inst_disk[100] + inst_disk[40]
            for inst in inst_list:
                rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst) + ',' + 'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

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
                            machine_rest_null_3000.remove(m)
                            break
        else:
            break

def alloc_40_100(inst_disk, machine_rest_null,
                   machineApps, machineResource, appResource,
                   appInterference, inst2App, res, inst2Machine):
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
    random.shuffle(inst_list)
    while len(inst_list)!=0:
        for inst in inst_list:
            for m in machine_rest_null:
                rest_tmp, logitic = calOneMachineRest(inst, m - 1, machineResource, appResource, inst2App)
                # 判断nachine剩余资源状态
                if logitic == True:  # 可以容下
                    if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束

                        inst2Machine[inst] = m
                        res.append('inst_' + str(inst)+','+'machine_' + str(m))  # write magrite processment

                        machineApps[m].append(inst2App[inst])
                        machineResource[m - 1] = rest_tmp

                        inst_list.remove(inst)

                        print(inst, m, len(inst_list), len(machine_rest_null))
                        if rest_tmp[196] < 40:  # 判断disk 是否够用
                            print(rest_tmp[196])
                            machine_rest_null.remove(m)
                        break # 放完一个inst就退出该层循环


def main():
    start = time.clock()
    inst2App, \
    inst2Machine, \
    machineApps, \
    machineInsts, \
    appResource, \
    appInterference, \
    machineResource = loaddata()

    # check app_inter conflict
    machine_conflict = checkConflict(appInterference, machineApps, machineInsts, inst2App)

    # magrite conflicted machine's all instances
    res = magrite(machine_conflict, machineApps, machineInsts,
                  inst2Machine, machineResource, appResource,
                  inst2App, appInterference)

    machine_rest_not_null_3000, \
    machine_rest_not_null_0, \
    machine_rest_null_3000, \
    machine_rest_null_0 = claasifyMachine(machineResource)

    magriteF1_F2(machine_rest_null_0, machine_rest_not_null_0,
                 machineApps, machineInsts,
                 inst2Machine, machineResource, appResource,
                 inst2App, appInterference, res)

    # rest insts to alloc
    inst_to_alloc = underAllocInst(inst2Machine)

    machine_rest_not_null_3000, \
    machine_rest_not_null_0, \
    machine_rest_null_3000, \
    machine_rest_null_0 = claasifyMachine(machineResource)

    # 首先把非空的规格为【92,288,1024】机器先放完

    for inst in inst_to_alloc:
        for m in machine_rest_not_null_3000:
            rest_tmp, logitic = calOneMachineRest(inst, m-1, machineResource, appResource, inst2App)
            # 判断nachine剩余资源状态
            if logitic == True:  # 可以容下
                if checkAppInter(inst, m, appInterference, machineApps, inst2App):  # 满足app约束
                    inst2Machine[inst] = m
                    res.append('inst_' + str(inst)+','+'machine_' + str(m))  # write magrite processment
                    machineApps[m].append(inst2App[inst])
                    machineResource[m - 1] = rest_tmp
                    inst_to_alloc.remove(inst)
                    #inst_to_0.remove(inst)
                    print(inst, m, len(machine_rest_not_null_3000))
                    # break # 放完一个inst就退出该层循环
                    if rest_tmp[196] < 40:  # 判断disk 是否够用
                        print(rest_tmp[196])
                        machine_rest_not_null_3000.remove(m)
                    break

    inst_disk = classifyInstByDisk(inst_to_alloc, inst2App, appResource)

    alloc_120_1024(inst_disk, machine_rest_null_3000,
                   machineApps, machineResource, appResource,
                   appInterference, inst2App, res, inst2Machine)

    machine_rest_null = machine_rest_null_3000 + machine_rest_null_0
    alloc_40_100(inst_disk, machine_rest_null,
                 machineApps, machineResource, appResource,
                 appInterference, inst2App, res, inst2Machine)

    inst_rest = underAllocInst(inst2Machine)
    end = time.clock()
    print('time use:', end-start)
    write2file(res)
    print()
    '''
    java -jar judge.jar problem.csv submit.csv
    '''

if __name__ == '__main__':
    main()
