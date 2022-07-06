# -*- coding: utf-8 -*-
import paramiko
import re
import yaml
import time

class ReadConfig():
    def __init__(self):
        self.yaml_name = "./config.yaml"
        self.yaml_info = self.read_yaml()
        self.yaml_list = self.get_list()

    def read_yaml(self):
        try:
            with open(self.yaml_name,encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except FileNotFoundError:
            print(f"配置文件读取错误，请检查配置文件名: {self.yaml_name}")
        except TypeError:
            print("配置文件读取错误，请检查输入的类型")

    def get_list(self):
        list = []
        for node in self.yaml_info["node"]:
            list.append([node['name'], node['ip'],'root',node['password']])
        return list

class Ssh():
    def __init__(self,name,ip,username,password,port=22):
        self.name = name
        self.ip = ip
        self.username = username
        self.password = password
        self.port = port
        self.SSHConnection = None
        self.connect()

    def connect(self):
            objSSHClient = paramiko.SSHClient()  # 创建SSH对象
            objSSHClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 允许连接其他主机
            objSSHClient.connect(hostname=self.ip,
                                 port=self.port,
                                 username=self.username,
                                 password=self.password,)  # 连接服务器
            self.SSHConnection = objSSHClient

    def exec_command(self,command):
        if self.SSHConnection:
            stdin, stdout, stderr = self.SSHConnection.exec_command(command)
            data = stdout.read()
            data = data.decode('utf-8') #此处注意，原始输出编码为bytes-like，但使用正则表达式findall()则需要chart-like,需要改编码
            return data

    def close(self):
        self.SSHConnection.close()


class CreateResource():

    def resource_definition(self):
        cmd = f'linstor resource-definition create giresource'
        return cmd

    def volume_definiton(self,size):
        cmd = f'linstor volume-definition create giresource {size}'
        return cmd

    def create_diskful_resource(self,node_name,sp):
        cmd = f'linstor resource create {node_name} giresource --storage-pool {sp}'
        return cmd


class PerformCreateResourceTask():
    def __init__(self):
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']

    def check_resource_status(self,ssh_obj):
        cmd = f'linstor r l | grep giresource'
        info = ssh_obj.exec_command(cmd)
        return info

    def check_nodeid_and_volume(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        info = ssh_obj.exec_command(f'drbdsetup status giresource -vs')
        ssh_obj.close()

        node1 = 'giresource node-id:'
        node2 = f'{node2_name} node-id:'
        volume = 'volume:'

        first_nodeid_list = re.findall(node1 + r'(\d)', info)
        secound_nodeid_list = re.findall(node2 + r'(\d)', info)
        volume_list = re.findall(volume + r'(\d)', info)
        del volume_list[1]

        list1 = []
        list2 = []

        list1.append(first_nodeid_list[0])
        list1.append(volume_list[0])
        list2.append(secound_nodeid_list[0])
        list2.append(volume_list[1])

        list3 = {}
        list3[node1_name] = list1
        list3[node2_name] = list2

        return list3

    def step1(self):
        state = False
        print("step1:创建指定的两个diskful节点")
        try:
            ssh_obj_create = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
            ssh_obj_create.exec_command('linstor resource-definition create giresource')
            ssh_obj_create.exec_command(f'linstor volume-definition create giresource {self.size}')
            ssh_obj_create.close()
            print("resource和volume资源创建成功")
        except:
            print("resource和volume资源创建失败")

        for node in self.yaml_node_list[1:]:
            try:
                ssh_obj = Ssh(node[0],node[1],node[2],node[3])
                print(f"节点{node[0]}连接成功")
                try:
                    ssh_obj.exec_command(f'linstor resource create {node[0]} giresource --storage-pool {self.sp}')
                    print(f'节点{node[0]}的diskful资源创建成功')
                    ssh_obj.close()
                    state = True
                except:
                    print(f'节点{node[0]}的diskful资源创建失败')
                    state = False
            except:
                print(f"节点{node[0]}连接失败，step1失败")
                state = False
        return state

    def step2(self):
        print("step2:检查giresource资源情况")
        print("现在开始执行giresource资源检查步骤")
        ssh_obj = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
        info = self.check_resource_status(ssh_obj)
        ssh_obj.close()
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        test1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
        test2 = re.findall(r'(%s)[\w\W]*(SyncTarget)' % node2_name, info)
        if test1[0][0] == node1_name and test2[0][0] == node2_name:
            print(f"{node1_name}的状态为 UpToDate")
            print(f"{node2_name}的状态为 SyncTarget")
            state = True
        else:
            print(f'{node1_name}或{node2_name}的状态错误，请检查')
            state = False
        return state

    def step3(self):
        print("step3:检查GI Tuple信息")
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'
        node2_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node1_name][0]} {nodeid_and_volume_info[node1_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        ssh_obj_2 = Ssh(self.yaml_node_list[2][0],self.yaml_node_list[2][1],self.yaml_node_list[2][2],self.yaml_node_list[2][3])
        GI_info2 = ssh_obj_2.exec_command(node2_GI_query_cmd)
        ssh_obj_2.close()

        result1 = re.findall(r'[\w]{16}',GI_info1)
        result2 = re.findall(r'[\w]{16}',GI_info2)
        print(result2)
        print(result1)

        if result2[0] == result1[1] :
            print("同步源的Bitmap UUID和同步目标的Current相同的")
            state = True
        else:
            print("同步源的Bitmap UUID和同步目标的Current不同，出现错误")
            state = False
        return state

    def step4(self):
        print("step4:创建diskless")
        state = False
        cmd = 'linstor resource create ubuntu giresource --diskless'
        try:
            ssh_obj = Ssh(self.yaml_node_list[0][0],self.yaml_node_list[0][1],self.yaml_node_list[0][2],self.yaml_node_list[0][3])
            ssh_obj.exec_command(cmd)
            ssh_obj.close()
            state = True
            print("diskless创建成功")
        except:
            print("diskless创建失败")

        return state

    def start_up(self):
        print("开始执行资源创建")
        state1 = self.step1()
        if state1 is True:
            state2 = self.step2()
            if state2 is True:
                state3 = self.step3()
                if state3 is True:
                    state4 = self.step4()
                    if state4 is True:
                        print("资源创建完成")
                    else:
                        print("step4失败")
                else:
                    print("step3失败")
            else:
                print("step2失败")
        else:
            print("step1失败")

class SyncCheck():
    def __init__(self):
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']

    def check_nodeid_and_volume(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        info = ssh_obj.exec_command(f'drbdsetup status giresource -vs')
        ssh_obj.close()

        node1 = 'giresource node-id:'
        node2 = f'{node2_name} node-id:'
        volume = 'volume:'

        first_nodeid_list = re.findall(node1 + r'(\d)', info)
        secound_nodeid_list = re.findall(node2 + r'(\d)', info)
        volume_list = re.findall(volume + r'(\d)', info)
        del volume_list[1]

        list1 = []
        list2 = []

        list1.append(first_nodeid_list[0])
        list1.append(volume_list[0])
        list2.append(secound_nodeid_list[0])
        list2.append(volume_list[1])

        list3 = {}
        list3[node1_name] = list1
        list3[node2_name] = list2

        return list3

    def linstor_sync_check(self):
        print("开始检测linstor集群同步情况")
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        ssh_obj = Ssh(self.yaml_node_list[1][0],self.yaml_node_list[1][1],self.yaml_node_list[1][2],self.yaml_node_list[1][3])
        info = ssh_obj.exec_command('linstor r l | grep giresource')
        result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
        result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)

        a = False
        while a is False:
            try:
                if result1[0][0] == node1_name and result2[0][0] == node2_name :
                    ssh_obj.close()
                    print("linstor集群同步完成")
                    break
                else:
                    print("result数组数据有误")
                    break
            except:
                time.sleep(10)
                info = ssh_obj.exec_command('linstor r l | grep giresource')
                result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
                result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
                print('linstor集群同步中')
                continue

    def gituple_check(self):
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]} '
        node2_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node1_name][0]} {nodeid_and_volume_info[node1_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                        self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        ssh_obj_2 = Ssh(self.yaml_node_list[2][0], self.yaml_node_list[2][1], self.yaml_node_list[2][2],
                        self.yaml_node_list[2][3])
        GI_info2 = ssh_obj_2.exec_command(node2_GI_query_cmd)
        ssh_obj_2.close()

        result1 = re.findall(r'[\w]{16}', GI_info1)
        result2 = re.findall(r'[\w]{16}', GI_info2)

        if result2[0] == result1[0] and result2[1] == result1[1]:
            print("同步目标的Current UUID、Bitmap UUID和同步源的Current UUID、Bitmap UUID一致")
            state = True
        else:
            print("同步目标的Current UUID、Bitmap UUID和同步源的Current UUID、Bitmap UUID不同，出现错误")
            state = False
        return state





if __name__ == "__main__":
    '''
    1.在被指定为diskful的节点，分别创建resource和volume
    linstor rd d giresource
    '''

    test = PerformCreateResourceTask()
    test2 = SyncCheck()
    test.start_up()
    test2.linstor_sync_check()
    test2.gituple_check()


