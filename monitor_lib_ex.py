# -*- coding:utf-8 -*-

import socket
import time
import json
import psutil


# def get_cpu_status(path='/proc/loadavg'):
#     '''
#         cpu usage
#     '''
#     loadavg = {}

#     with open(path, 'r') as f1:

#         list_content = f1.read().split()

#         loadavg['lavg_1'] = list_content[0]

#         loadavg['lavg_2'] = list_content[1]

#         loadavg['lavg_15'] = list_content[2]

#     return loadavg

def get_cpu_status(path='/proc/loadavg'):
    '''
        cpu usage
    '''
    loadavg = {}

    Load1, Load5, Load15 = psutil.getloadavg()

    loadavg['lavg_1'] = Load1

    loadavg['lavg_2'] = Load5

    loadavg['lavg_15'] = Load15

    return loadavg


# def get_memory_status(path='/proc/meminfo'):
#     '''
#        memory usage
#     '''
#     mem_dic = {}

#     with open(path, 'r') as f2:

#         lines = f2.readlines()

#         for line in lines:

#             name = line.strip().split(':')[0]

#             data = line.split(":")[1].split()[0]

#             mem_dic[name] = float(data)

#     return mem_dic

def get_memory_status():
    """
    Get memory usage using psutil.
    """
    mem_info = psutil.virtual_memory()
    mem_dic = {
        'total': mem_info.total / 1024,  # Convert to KB
        'available': mem_info.available / 1024,
        'used': mem_info.used / 1024,
        'free': mem_info.free / 1024,
        'buffers': mem_info.buffers / 1024 if hasattr(mem_info, 'buffers') else 0,
        'cached': mem_info.cached / 1024 if hasattr(mem_info, 'cached') else 0,
        'swap_total': psutil.swap_memory().total / 1024,
        'swap_free': psutil.swap_memory().free / 1024,
    }
    return mem_dic

if __name__=='__name__':
    cpu = get_cpu_status()
    mem = get_memory_status()
	
    print(cpu)
    print('-----------')
    print(mem)

