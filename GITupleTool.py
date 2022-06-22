# -*- coding: utf-8 -*-
import paramiko
import re
import yaml

def get_GI_command(resource,node_id,volume):
    """
    构造 drbdsetup show-gi resource node-id volume 命令
    """

    GI_command = f'drbdsetup show-gi {resource} {node_id} {volume}'
    return  GI_command

def step1_filter_data(data):

    pass




def step2_filter_data(data):

    pass



class ReadConfig() :
    def __init__(self):
        self.yaml_name = "./config.yaml"
        self.yaml_info = self.read_yaml()
        self.resource = self.yaml_info["resource"]
        self.resource_cmd = self.get_resource_cmd()

    def read_yaml(self):
        try:
            with open(self.yaml_name,encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except FileNotFoundError:
            print(f"Please check the file name: {self.yaml_name}")
        except TypeError:
            print("Error in the type of file name.")

    def get_list(self):
        list = []
        for node in self.yaml_info["node"]:
            list.append([node['name'], node['ip'],'root',node['password']])
        return list

    def get_resource_cmd(self):
        resource_cmd = f'drbdsetup status {self.resource} -vs'
        return resource_cmd

class Ssh() :
    def __init__(self,name,ip,username,password,port=22,):
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
            return data


if __name__ == "__main__":
    """
    1.实例化读config文件的类
    2.将config文件中node节点的相关信息整理以列表输出 a
    3.在读config文件的类中构造第一条命令，即查寻各个节点的node-id和volume
    4.ssh进入到首选的第一个节点，输入命令，输出信息
    5.拿到输出的信息进行第一次筛选，筛选输出结果包含各个节点对应的node-id和volume
    6.根据第一次筛选的输出结果，构造第二条命令，每个节点应输入所有节点-1条命令
    7.ssh进入到每个节点，输入第二次构造的命令，搜集每一次执行命令后的信息，输入并进行保存
    8.根据保存的记录和结果，使用一个函数或类来把信息整合排列并进行第二次筛选后输出
    """
    test_read = ReadConfig()
    a = test_read.get_list()
    print(test_read.resource_cmd)
    one_ssh = Ssh(a[0][0],a[0][1],a[0][2],a[0][3])
    result = one_ssh.exec_command(test_read.resource_cmd)
    print(result)


    # for i in a :
    #     test_ssh = Ssh(i[0],i[1],i[2],i[3])

