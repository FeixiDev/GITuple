# -*- coding: utf-8 -*-
import paramiko
import subprocess
import yaml


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
    test_read = ReadConfig()
    a = test_read.get_list()
    print(test_read.resource_cmd)
    one_ssh = Ssh(a[0][0],a[0][1],a[0][2],a[0][3])
    result = one_ssh.exec_command(test_read.resource_cmd)
    print(result)


    # for i in a :
    #     test_ssh = Ssh(i[0],i[1],i[2],i[3])

