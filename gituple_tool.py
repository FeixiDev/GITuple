# -*- coding: utf-8 -*-
import sys
import paramiko
import re
import yaml
import time
import subprocess
import logging
import datetime
from threading import Thread
import timeout_decorator
import traceback


class ReadConfig():
    def __init__(self):
        self.yaml_name = "./config.yaml"
        self.yaml_info = self.read_yaml()
        self.yaml_list = self.get_list()

    def read_yaml(self):
        try:
            with open(self.yaml_name, encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except FileNotFoundError:
            print(f"配置文件读取错误，请检查配置文件名: {self.yaml_name}")
        except TypeError:
            print("配置文件读取错误，请检查输入的类型")

    def get_list(self):
        list = []
        for node in self.yaml_info["node"]:
            list.append([node['name'], node['ip'], 'root', node['password']])
        return list


class Ssh():
    def __init__(self, name, ip, username, password, port=22):
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
                             password=self.password, )  # 连接服务器
        self.SSHConnection = objSSHClient

    def exec_command(self, command):
        if self.SSHConnection:
            stdin, stdout, stderr = self.SSHConnection.exec_command(command)
            data = stdout.read()
            data = data.decode('utf-8')  # 此处注意，原始输出编码为bytes-like，但使用正则表达式findall()则需要chart-like,需要改编码
            return data

    def close(self):
        self.SSHConnection.close()


class CreateResource():

    def resource_definition(self):
        cmd = f'linstor resource-definition create giresource'
        return cmd

    def volume_definiton(self, size):
        cmd = f'linstor volume-definition create giresource {size}'
        return cmd

    def create_diskful_resource(self, node_name, sp):
        cmd = f'linstor resource create {node_name} giresource --storage-pool {sp}'
        return cmd


class PerformCreateResourceTask():
    """
    第一步，创建资源
    """
    def __init__(self):
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']

    def check_resource_status(self, ssh_obj):
        cmd = f'linstor r l -p| grep giresource'
        info = ssh_obj.exec_command(cmd)
        return info

    def check_nodeid_and_volume(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
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
        state = True
        print("step1:创建指定的两个diskful节点")
        try:
            ssh_obj_create = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                                 self.yaml_node_list[1][3])
            ssh_obj_create.exec_command('linstor resource-definition create giresource')
            ssh_obj_create.exec_command(f'linstor volume-definition create giresource {self.size}')
            ssh_obj_create.close()
            print("resource和volume资源创建成功")
        except:
            logging.warning("resource和volume资源创建失败")
            print("resource和volume资源创建失败")

        for node in self.yaml_node_list[1:]:
            ssh_obj = Ssh(node[0], node[1], node[2], node[3])
            print(f"节点{node[0]}连接成功")
            ssh_obj.exec_command(f'linstor resource create {node[0]} giresource --storage-pool {self.sp}')
            print(f'节点{node[0]}的diskful资源尝试创建')

        return state

    def step2(self):
        print("step2:检查giresource资源情况")
        print("现在开始执行giresource资源检查步骤")
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = self.check_resource_status(ssh_obj)
        ssh_obj.close()
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        try:
            result1 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % str(node1_name), str(info))
            result2 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % str(node2_name), str(info))
            result1_1 = re.findall(r'([a-z|A-Z]*)', result1[0][1].strip())
            result1_2 = result1_1[0].strip()
            result2_1 = re.findall(r'([a-z|A-Z]*)', result2[0][1].strip())
            result2_2 = result2_1[0].strip()
            print('资源创建成功')
        except:
            logging.warning("资源创建失败")
            print('资源创建失败')

        time.sleep(4)
        if result1_2 == 'UpToDate' and result2_2 == 'SyncTarget':
            print(f"{node1_name}的状态为 UpToDate")
            print(f"{node2_name}的状态为 SyncTarget")
            state = True
        else:
            logging.warning(f"{node1_name}的状态为{result1_2},错误；{node2_name}的状态为{result2_2},错误")
            print(f"{node1_name}的状态为{result1_2},错误")
            print(f"{node2_name}的状态为{result2_2},错误")
            state = False
            sys.exit()
        return state

    def step3(self):
        print("step3:检查GI Tuple信息")
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'
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

        if result2[0][0:15] == result1[1][0:15]:
            print(f'同步目标的Current为{result2[0]}')
            print(f'同步源的Bitmap为{result1[1]}')
            print("同步源的Bitmap UUID和同步目标的Current相同")
            logging.info(f'  (1)期望:同步源的Bitmap UUID和同步目标的Current相同\n')
            logging.info(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n{node2_name}的GI为:{GI_info1}\n{node1_name}的GI为:{GI_info2}\n\n')
            state = True
        else:
            print(f'同步目标的Current为{result2[0]}')
            print(f'同步源的Bitmap为{result1[1]}')
            print("同步源的Bitmap UUID和同步目标的Current不同，出现错误")
            logging.warning('  (1)期望:同步源的Bitmap UUID和同步目标的Current相同\n')
            logging.warning('  (2)实际情况:与预期不符,但小资源创建最后一位差1一位数为正常情况\n')
            logging.warning(f'  (3)测试结果:\n{node2_name}的GI为:{GI_info1}{node1_name}的GI为:{GI_info2}\n\n')
            state = False
            sys.exit()
        return state

    def step4(self):
        print("step4:创建diskless")
        state = False
        cmd = f'linstor resource create {self.yaml_node_list[0][0]} giresource --diskless'
        try:
            ssh_obj = Ssh(self.yaml_node_list[0][0], self.yaml_node_list[0][1], self.yaml_node_list[0][2],
                          self.yaml_node_list[0][3])
            ssh_obj.exec_command(cmd)
            print("diskless创建成功")
            state = True
        except:
            logging.warning("diskless创建失败")
            print("diskless创建失败")
            state = False
            sys.exit()

        return state

    def start_up(self):
        logging.warning('1.创建资源\n')
        print("开始执行资源创建")
        state1 = self.step1()
        if state1 is True:
            time.sleep(5)
            state2 = self.step2()
            if state2 is True:
                state3 = self.step3()
                if state3 is True:
                    state4 = self.step4()
                    if state4 is True:
                        print("资源创建完成")
                        return True
                    else:
                        print("step4失败")
                        return False
                else:
                    print("step3失败")
                    return False
            else:
                print("step2失败")
                return False
        else:
            print("step1失败")
            return False

# @timeout_decorator.timeout(3600)
class SyncCheck():
    """
    第二部,检查同步情况
    """
    def __init__(self):
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']

    def check_nodeid_and_volume(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
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

    @timeout_decorator.timeout(3600)
    def linstor_sync_check(self):
        print("开始检测linstor集群同步情况")
        node1_name = self.yaml_node_list[1][0]
        node2_name = self.yaml_node_list[2][0]
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = ssh_obj.exec_command('linstor r l | grep giresource')
        result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
        result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
        result1_1 = re.findall(r'(%s)[\w\W]*(Inconsistent)' % node1_name, info)
        result2_1 = re.findall(r'(%s)[\w\W]*(Inconsistent)' % node1_name, info)

        a = False
        while a is False:
            try:
                if result1[0][0] == node1_name and result2[0][0] == node2_name:
                    ssh_obj.close()
                    print("linstor集群同步完成")
                    break
                else:
                    print("result数组数据有误,linstor集群同步失败")
                    break

            except:
                time.sleep(30)
                info = ssh_obj.exec_command('linstor r l | grep giresource')
                result1 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node1_name, info)
                result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
                print('linstor集群同步中........')
                continue
        return True

    def gituple_check(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
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
        print(f'{node1_name}的gi为 {result1}')
        print(f'{node2_name}的gi为 {result2}')

        if result2[0] == result1[0] and result2[1] == result1[1]:
            print("两节点的Current UUID、Bitmap UUID和Current UUID、Bitmap UUID一致")
            logging.warning(f'  (1)期望:两节点的Current UUID、Bitmap UUID和Current UUID、Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n{node2_name}的GI为:{GI_info1}{node1_name}的GI为:{GI_info2}\n\n')
            state = True
        else:
            logging.warning(f'  (1)期望:两节点的Current UUID、Bitmap UUID和Current UUID、Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期不符\n')
            logging.warning(f'  (3)测试结果:\n{node2_name}的GI为:{GI_info1}{node1_name}的GI为:{GI_info2}\n\n')
            print("两节点的Current UUID、Bitmap UUID和Current UUID、Bitmap UUID不同，出现错误")
            state = False    #此处应为false
            sys.exit()
        return state

    def start_up(self):
        logging.warning('等待同步完成\n')
        state1 = self.linstor_sync_check()
        if state1 is True:
            state2 = self.gituple_check()
            if state2 is True:
                return True
            else:
                return False
        else:
            return False


class DdWriteData(SyncCheck):
    """
    第三步,dd写数据
    """
    def __init__(self):
        super(DdWriteData, self).__init__()
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']

    def get_devicename(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = ssh_obj.exec_command("linstor r lv | grep giresource")
        ssh_obj.close()
        data = re.findall(f'\|([\w\s\/]+)', info)
        data1 = data[5]
        data2 = data1.strip()
        print(f"devicename已获取，为{data2}")
        return data2

    def use_dd_to_write_data(self): #用多线程重写
        try:
            devicename = self.get_devicename()
            print("................................线程1:开始执行dd写数据操作")
            cmd = f'dd if=/dev/urandom of={devicename} oflag=direct status=progress'
            ssh_obj = Ssh(self.yaml_node_list[0][0], self.yaml_node_list[0][1], self.yaml_node_list[0][2],
                          self.yaml_node_list[0][3])
            ssh_obj.exec_command(cmd)
            time.sleep(5)
            print(".....................dd写数据操作执行完毕,dd进程已被关闭")
        except:
            logging.warning("dd写数据操作执行失败")
            print("dd写数据操作执行失败")

    def start_up(self):
        logging.warning('dd写数据\n')
        state1 = Thread(target=self.use_dd_to_write_data)
        state1.setDaemon(True)
        state1.start()
        state2 = self.gituple_check()
        if state2 is True:
            return True
        else:
            return False


    # 下一步应运行父类gituple_check


class DrbdNetworkOperation(SyncCheck):
    """
    第四步,drbd网络操作
    1.down_interface,down的n2
    2.gituple_check_type1
    3.up_interface
    4.linstor_cluster_check
    5.gituple_check_type1
    """

    def __init__(self):
        super(DrbdNetworkOperation, self).__init__()
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']
        self.device = self.yaml_info['device']

    def down_interface(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        try:
            for dev in self.device:
                cmd = f'ifconfig {dev} down'
                ssh_obj.exec_command(cmd)
                print(f"{self.yaml_node_list[1][0]}的网卡：{dev}已经关闭")
            ssh_obj.close()
            state = True
        except:
            logging.warning("网卡关闭失败")
            print("网卡关闭失败")
            state = False
            sys.exit()
        return state

    def up_interface(self):
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        try:
            for dev in self.device:
                cmd = f'nmcli device connect {dev}'
                ssh_obj.exec_command(cmd)
                print(f"网卡：{dev}已经开启")
            ssh_obj.close()
            state = True
        except:
            logging.warning("网卡开启失败")
            print("网卡开启失败")
            state = False
            sys.exit()
        return state

    def linstor_cluster_check(self):
        print("检测linstor集群情况")
        node1_name = self.yaml_node_list[1][0]  # n2
        node2_name = self.yaml_node_list[2][0]  # n3
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = ssh_obj.exec_command('linstor r l | grep giresource')
        ssh_obj.close()
        result1 = re.findall(r'(%s)[\w\W]*(Inconsistent)' % node1_name, info)
        result2 = re.findall(r'(%s)[\w\W]*(UpToDate)' % node2_name, info)
        try:
            if result1[0][0] == node1_name and result2[0][0] == node2_name:
                print(f'{node1_name}节点状态为Inconsistent，正常')
                state = True
            else:
                logging.warning("节点状态异常")
                print(f'{node1_name}节点状态异常')
                state = False
                sys.exit()
        except:
            logging.warning("节点状态异常")
            print(f'{node1_name}节点状态异常')
            state = False
            sys.exit()

        return state

    def start_up(self):
        logging.warning('\n')
        state1 = self.down_interface()
        time.sleep(10)
        if state1 is True:
            state2 = self.gituple_check_type1()
            if state2 is True:
                state3 = self.up_interface()
                time.sleep(15)
                if state3 is True:
                    state4 = self.linstor_cluster_check()
                    if state4 is True:
                        time.sleep(8)
                        state5 = self.gituple_check_type1()
                        logging.warning('\n\n')
                        if state5 is True:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

    def gituple_check_type1(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
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
        print(f'{node1_name}的Current UUID为{result1[0]}')
        print(f'{node2_name}的Bitmap UUID为{result2[1]}')
        if result2[1] == result1[0] :   #down的是n2，因此n2的Current UUID与n3的Bitmap UUID应一致
            print(f"节点{node1_name}的Current UUID与节点{node2_name}的Bitmap UUID一致")
            logging.warning(f'  (1)预期:节点{node1_name}的Current UUID与节点{node2_name}的Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n{node1_name}的GI信息为{GI_info1}\n{node2_name}的GI信息为{GI_info2}\n')

            state = True
        else:
            print(f"节点{node1_name}的Current UUID与节点{node2_name}的Bitmap UUID不一致，错误")
            logging.warning(f'  (1)预期:节点{node1_name}的Current UUID与节点{node2_name}的Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期不符\n')
            logging.warning(f'  (3)测试结果:\n{node1_name}的Current UUID为{result1[0]}\n{node2_name}的Bitmap UUID为{result2[1]}\n')
            state = False    #此处应为false
            sys.exit()
        return state

class StopDdAndCheckGituple(SyncCheck):
    """
    第五步,停dd
    1.stop_dd
    2.linstor_sync_check
    3.gituple_check
    """
    def __init__(self):
        super(StopDdAndCheckGituple, self).__init__()

    def stop_dd(self):
        print(f'停止diskless节点{self.yaml_node_list[0][0]}的写dd操作')
        try:
            ssh_obj = Ssh(self.yaml_node_list[0][0],self.yaml_node_list[0][1],self.yaml_node_list[0][2],self.yaml_node_list[0][3])
            process_info = ssh_obj.exec_command('ps -A | grep dd')
            test = re.findall(f'([\d]+) \?        \w\w:\w\w:\w\w dd',process_info)
            dd_pid = test[0]
            ssh_obj.exec_command(f'kill {dd_pid}')
            print("dd进程已终止")
            state = True
        except:
            logging.warning("停止dd写数据出现错误")
            print("停止dd写数据出现错误")
            state = False
            sys.exit()

        return state

    def start_up(self):
        logging.warning('停止dd并检查GI\n')
        state1 = self.stop_dd()
        time.sleep(5)
        if state1 is True:
            state2 = self.linstor_sync_check()
            if state2 is True:
                state3 = self.gituple_check()
                if state3 is True:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False

class NodeOperationMock(SyncCheck):
    def __init__(self):
        super(NodeOperationMock, self).__init__()
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.sp = self.yaml_info['sp']
        self.size = self.yaml_info['size']
        self.device = self.yaml_info['device']

    def down_interface(self):
        ssh_obj = Ssh(self.yaml_node_list[2][0], self.yaml_node_list[2][1], self.yaml_node_list[2][2],
                      self.yaml_node_list[2][3])
        try:
            for dev in self.device:
                cmd = f'ifconfig {dev} down'
                ssh_obj.exec_command(cmd)
                print(f"{self.yaml_node_list[2][0]}网卡：{dev}已经关闭")
            ssh_obj.close()
            state = True
        except:
            logging.warning("网卡关闭失败")
            print("网卡关闭失败")
            state = False
            sys.exit()
        return state

    def up_interface(self):
        ssh_obj = Ssh(self.yaml_node_list[2][0], self.yaml_node_list[2][1], self.yaml_node_list[2][2],
                      self.yaml_node_list[2][3])
        try:
            for dev in self.device:
                cmd = f'nmcli device connect {dev}'
                ssh_obj.exec_command(cmd)
                print(f"{self.yaml_node_list[2][0]}网卡：{dev}已经开启")
            ssh_obj.close()
            state = True
        except:
            logging.warning("网卡开启失败")
            print("网卡开启失败")
            state = False
            sys.exit()
        return state

    def linstor_cluster_check(self):
        print("检测linstor集群情况")
        node1_name = self.yaml_node_list[1][0]  # n2
        node2_name = self.yaml_node_list[2][0]  # n3
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = ssh_obj.exec_command('linstor r l -p | grep giresource')
        ssh_obj.close()
        result1 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % node1_name, str(info))
        result2 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % node2_name, str(info))
        print(result1)
        print(result2)
        result1_1 = result1[0][1]
        result1_2 = result1_1.strip()
        result2_1 = result2[0][1]
        result2_2 = result2_1.strip()
        try:
            if result1_2 == 'UpToDate' and result2_2 == 'Inconsistent':
                print(f'{node2_name}节点状态为{result2_2}，正常')
                state = True
            else:
                logging.warning("节点状态异常")
                print(f'{node2_name}节点状态异常,为 {result2_2}')
                state = False
                sys.exit()
        except:
            logging.warning("节点状态异常")
            print(f'{node2_name}节点状态异常,为 {result2_2}')
            state = False

        return state

    def gituple_check_type0(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
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

        result1 = re.findall(r'[\w]{16}', GI_info1) #n3的结果
        result2 = re.findall(r'[\w]{16}', GI_info2) #n2的结果
        print(result1)
        print(result2)

        if result2[0] == result1[1] :   #down的是n3，因此n3的Current UUID与n2的Bitmap UUID应一致
            print(f"节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致")
            logging.warning(f'  (1)预期:节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}在{node2_name}上执行{node2_GI_query_cmd}\n{GI_info2}\n\n')
            state = True
        else:
            print(f"节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID不一致，错误")
            logging.warning(f'  (1)预期:节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期不符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}在{node2_name}上执行{node2_GI_query_cmd}\n{GI_info2}\n\n')
            state = False    #应为False
            sys.exit()
        return state

    def gituple_check_type2(self):
        history_gi = self.gituple_return()
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                        self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        result1 = re.findall(r'[\w]{16}', GI_info1)
        print(result1)

        if history_gi == result1[1] :   #down的是n3，因此n3的Current UUID与n2的Bitmap UUID应一致
            print(f"节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致")
            logging.warning(f'  (1)预期:节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}\n')
            state = True
        else:
            print(f"节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}不一致，错误")
            logging.warning(f'  (1)预期:节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致\n')
            logging.warning(f'  (2)实际情况:与预期不符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}\n')
            state = False    #应为False
            sys.exit()
        return state

    def gituple_return(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                        self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        result1 = re.findall(r'[\w]{16}', GI_info1)
        result2 = result1[0]

        return result2

    def start_up(self):
        logging.warning('开关节点并检查GI\n')
        state1 = self.down_interface()
        if state1 is True:
            time.sleep(8)
            state2 = self.gituple_check_type2()
            if state2 is True:
                state3 = self.up_interface()
                if state3 is True:
                    time.sleep(15)
                    state5 = self.linstor_cluster_check()
                    if state5 is True:
                        stat6 = self.gituple_check_type0()
                        if stat6 is True:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

class NodeOperation(SyncCheck):
    def __init__(self):
        super(NodeOperation, self).__init__()
        self.obj_config = ReadConfig()
        self.yaml_info = self.obj_config.yaml_info
        self.yaml_node_list = self.obj_config.yaml_list
        self.yaml_phynode_list = self.yaml_info['phynode']
        self.ip = self.yaml_phynode_list[0]['ip']
        self.username = self.yaml_phynode_list[1]['username']
        self.password = self.yaml_phynode_list[2]['password']

    def down_interface(self):
        try:
            shutdown_cmd = f'ipmitool -I lanplus -H {self.ip} -U {self.username} -P {self.password} power off'
            check_shutdown_cmd = f'ipmitool -I lanplus -H {self.ip} -U {self.username} -P {self.password} power status'
            state1 = subprocess.run(shutdown_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,encoding="utf-8")
            time.sleep(5)
            state2 = subprocess.run(check_shutdown_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,encoding="utf-8")
            status_result = re.findall(r'Power is on', check_shutdown_cmd)
            if bool(status_result) is False:
                print("关机成功")
                print(state2)
                return True
            else:
                logging.warning("关机失败")
                print("关机失败")
                return False
        except:
            logging.warning("关机失败")
            print("关机失败")
            return False

    def up_interface(self):
        try:
            poweron_cmd = f'ipmitool -I lanplus -H {self.ip} -U {self.username} -P {self.password} power on'
            time.sleep(5)
            check_poweron_cmd = f'linstor n l'
            state1 = subprocess.run(poweron_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,encoding="utf-8")
            time.sleep(5)
            state2 = subprocess.run(check_poweron_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,encoding="utf-8")
            print("开机成功")
            print(state2)
            return True
        except:
            logging.warning("开机失败")
            print("开机失败")
            return False

    def linstor_cluster_check(self):
        print("检测linstor集群情况")
        node1_name = self.yaml_node_list[1][0]  # n2
        node2_name = self.yaml_node_list[2][0]  # n3
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        info = ssh_obj.exec_command('linstor r l -p | grep giresource')
        ssh_obj.close()
        result1 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % node1_name, str(info))
        result2 = re.findall(r'(%s)[\s]*\|[\w\s]*\|[\w\s]*\|[\w\s(),]*\|([\w\s().%%]*)\|' % node2_name, str(info))
        result1_1 = result1[0][1]
        result1_2 = result1_1.strip()
        result2_1 = result2[0][1]
        result2_2 = result2_1.strip()
        try:
            if result1_2 == 'UpToDate' and result2_2 == 'Inconsistent': #测试时有问题,应为UpToDate
                print(f'{node2_name}节点状态为{result2_2}，正常')
                state = True
            else:
                logging.warning("节点状态异常")
                print(f'{node2_name}节点状态异常,为 {result2_2}')
                state = False
                sys.exit()
        except:
            logging.warning("节点状态异常")
            print(f'{node2_name}节点状态异常,为 {result2_2}')
            state = False
            sys.exit()

        return state

    def gituple_check_type0(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
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

        result1 = re.findall(r'[\w]{16}', GI_info1) #n3的结果
        result2 = re.findall(r'[\w]{16}', GI_info2) #n2的结果

        if result2[0] == result1[1] :   #down的是n3，因此n3的Current UUID与n2的Bitmap UUID应一致
            print(f"节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致")
            logging.warning(f'  (1)预期:节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}在{node2_name}上执行{node2_GI_query_cmd}\n{GI_info2}\n\n')
            state = True
        else:
            print(f"节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID不一致，错误")
            logging.info(f'  (1)预期:节点{node2_name}的Current UUID与节点{node1_name}的Bitmap UUID一致\n')
            logging.info(f'  (2)实际情况:与预期不符\n')
            logging.info(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}在{node2_name}上执行{node2_GI_query_cmd}\n{GI_info2}\n\n')
            state = False    #应为False
            sys.exit()
        return state

    def gituple_check_type2(self):
        history_gi = self.gituple_return()
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                        self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        result1 = re.findall(r'[\w]{16}', GI_info1)

        if history_gi == result1[1] :   #down的是n3，因此n3的Current UUID与n2的Bitmap UUID应一致
            print(f"节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致")
            logging.warning(f'  (1)预期:节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致\n')
            logging.warning(f'  (2)实际情况:与预期相符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}\n')
            state = True
        else:
            print(f"节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}不一致，错误")
            logging.warning(f'  (1)预期:节点{node2_name}的原Current UUID{history_gi}与现Bitmap UUID{result1[1]}一致\n')
            logging.warning(f'  (2)实际情况:与预期不符\n')
            logging.warning(f'  (3)测试结果:\n在{node1_name}上执行{node1_GI_query_cmd}\n{GI_info1}\n')
            state = False    #应为False
            sys.exit()
        return state

    def gituple_return(self):
        node1_name = self.yaml_node_list[1][0]  #n2
        node2_name = self.yaml_node_list[2][0]  #n3
        nodeid_and_volume_info = self.check_nodeid_and_volume()
        node1_GI_query_cmd = f'drbdsetup get-gi giresource {nodeid_and_volume_info[node2_name][0]} {nodeid_and_volume_info[node2_name][1]}'

        ssh_obj_1 = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                        self.yaml_node_list[1][3])
        GI_info1 = ssh_obj_1.exec_command(node1_GI_query_cmd)
        ssh_obj_1.close()

        result1 = re.findall(r'[\w]{16}', GI_info1)
        result2 = result1[0]

        return result2

    def start_up(self):
        logging.info('\n')
        state1 = self.down_interface()
        time.sleep(5)
        if state1 is True:
            state2 = self.gituple_check_type2()
            if state2 is True:
                state3 = self.up_interface()
                time.sleep(15)
                if state3 is True:
                    state5 = self.linstor_cluster_check()
                    time.sleep(5)
                    if state5 is True:
                        stat6 = self.gituple_check_type0()
                        if stat6 is True:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

class DeleteResource(SyncCheck):
    def __init__(self):
        super(DeleteResource, self).__init__()

    def start_up(self):
        cmd = 'linstor rd d giresource'
        ssh_obj = Ssh(self.yaml_node_list[1][0], self.yaml_node_list[1][1], self.yaml_node_list[1][2],
                      self.yaml_node_list[1][3])
        try:
            ssh_obj.exec_command(cmd)
            print("资源删除成功")
            ssh_obj.close()
            return True
        except:
            logging.warning("资源删除失败")
            print("资源删除失败")
            return False

def log():
    time1 = datetime.datetime.now().strftime('%Y%m%d%H_%M_%S')
    # 此处进行Logging.basicConfig() 设置，后面设置无效
    logging.basicConfig(filename=f'{time1}_log.log',
                     format = '%(asctime)s - %(message)s',
                     level=logging.WARNING)

def operations():
    '''
    1.创建资源
    2.检查同步
    3.dd写数据
    4.n2网卡操作
    5.停dd
    6.dd写数据
    7.n3网卡操作
    8.停dd
    9.删除资源
    '''

    config = ReadConfig()
    status01 = config.yaml_info['phynode'][0]['ip']
    if bool(status01) is True:
        test6 = NodeOperation()
        print('使用物理节点断开操作')
    else:
        test6 = NodeOperationMock()
        print('使用断网卡模拟物理节点断开操作')


    test = PerformCreateResourceTask()
    test2 = SyncCheck()
    test3 = DdWriteData()
    test4 = DrbdNetworkOperation()
    test5 = StopDdAndCheckGituple()
    test7 = DeleteResource()

    step1 = test.start_up()
    if step1 is True:
        logging.info('2.等待同步完成')
        step2 = test2.start_up()
        if step2 is True:
            logging.info('3.dd写数据')
            step3 = test3.start_up()
            if step3 is True:
                logging.warning('4.进行节点开关网卡操作')
                step4 = test4.start_up()
                if step4 is True:
                    logging.warning('5.停止dd并检查GI')
                    step5 = test5.start_up()
                    if step5 is True:
                        logging.warning('6.dd写数据')
                        step6 = test3.start_up()
                        if step6 is True:
                            logging.warning('7.开关节点并检查GI')
                            step7 = test6.start_up()
                            if step7 is True:
                                logging.warning('8.停止dd并检查GI')
                                step8 = test5.start_up()
                                if step8 is True:
                                    step9 = test7.start_up()
                                    if step9 is True:
                                        print("成功，流程完成")
                                    else:
                                        print("错误")
                                        sys.exit()
                                else:
                                    print("错误")
                                    sys.exit()
                            else:
                                print("错误")
                                sys.exit()
                        else:
                            print("错误")
                            sys.exit()
                    else:
                        print("错误")
                        sys.exit()
                else:
                    print("错误")
                    sys.exit()
            else:
                print("错误")
                sys.exit()
        else:
            print("错误")
            sys.exit()
    else:
        print("错误")
        sys.exit()

def main():
    log()
    config_obj = ReadConfig()
    times = config_obj.yaml_info["Cycle execution times"]
    for i in range(times):
        print(f'--------开始第{i+1}次执行--------')
        logging.warning(f'--------开始第{i+1}次执行--------')
        operations()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.warning(traceback.format_exc())
        print(traceback.format_exc())