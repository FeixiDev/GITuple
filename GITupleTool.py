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


def step1_filter_data(data,first_name):
    """
    处理拿到的数据，将其构造为字典，格式为：{"name":["ndoe-id","volume"],.....}
    """

    node = "node-id:"
    volume = "volume:"

    nodeid_list = re.findall(node+r'(\d)',data)
    volume_list = re.findall(volume+r'(\d)',data)
    nodeid_new = re.findall(r' ([\w]{2}) '+node+'\d',data)

    list1 = []
    list2 = []

    for x ,y in zip(nodeid_list,volume_list) :
        list1.append(x)
        list1.append(y)
        list2.append(list1)
        list1 = []

    results = {}
    results[first_name] = list2[0]

    for x ,y in zip(nodeid_new,range(1,len(list2))) :
        results[x] = list2[y]

    return results


def step2_filter_data(data):
    """
    处理拿到的数据，提取GI元祖并返回
    """

    result = re.findall(r'\w{16}:\w{16}:\w{16}:\w{16}:\w:\w:\w:\w:\w:\w:\w:\w:\w:\w:\w:\w',data)

    return result

def final_output():
    """
    最后处理步骤，输出结果
    """
    config_read = ReadConfig()
    config_info = config_read.get_list()  #将关键信息提取并构造为一个数组
    config_resource = config_read.resource

    one_ssh = Ssh(config_info[0][0],config_info[0][1],config_info[0][2],config_info[0][3])  #取配置文件的第一个node节点信息来获取node-id和volume数据
    step1_result = one_ssh.exec_command(config_read.resource_cmd)  #取配置文件的第一个node节点信息来获取node-id和volume数据

    step1_info = step1_filter_data(step1_result,config_info[0][0])


    for i in config_info :
        i_ssh = Ssh(i[0],i[1],i[2],i[3])
        print(f'{i[0]}节点上的结果：')
        for z in step1_info :
            if i[0] != z :
                GI_command = get_GI_command(config_resource,step1_info[z][0],step1_info[z][1])
                GI_results = step2_filter_data(i_ssh.exec_command(GI_command))[0]
                print(f'命令：{GI_command}')
                print(f'GI元组：{GI_results}')




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
            data = data.decode('utf-8') #此处注意，原始输出编码为bytes-like，但使用正则表达式findall()则需要chart-like,需要改编码
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

    final_output()

    # config_read = ReadConfig()
    # config_info = config_read.get_list()  #将关键信息提取并构造为一个数组
    # config_resource = config_read.resource
    #
    # one_ssh = Ssh(config_info[0][0],config_info[0][1],config_info[0][2],config_info[0][3])  #取配置文件的第一个node节点信息来获取node-id和volume数据
    # step1_result = one_ssh.exec_command(config_read.resource_cmd)  #取配置文件的第一个node节点信息来获取node-id和volume数据
    #
    # step1_info = step1_filter_data(step1_result,config_info[0][0])
    #
    #
    # for i in config_info :
    #     i_ssh = Ssh(i[0],i[1],i[2],i[3])
    #     print(f'{i[0]}节点上的结果：')
    #     for z in step1_info :
    #         if i[0] != z :
    #             GI_command = get_GI_command(config_resource,step1_info[z][0],step1_info[z][1])
    #             GI_results = step2_filter_data(i_ssh.exec_command(GI_command))[0]
    #             print(f'命令：{GI_command}')
    #             print(f'GI元组：{GI_results}')
