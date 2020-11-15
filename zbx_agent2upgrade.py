# -*- coding: utf-8 -*-


# Author: AcidGo
# Usage:
#   url: 指定安装包的资源路径。


from __future__ import print_function
import platform, sys, os, time
import logging
import re
import subprocess


# CONFIG
AGENT2_PATH = "/usr/sbin/zabbix_agent2"
AGENT2_CONF = "/etc/zabbix/zabbix_agent2.conf"
AGENTD_PATH = "/usr/sbin/zabbix_agentd"
AGENTD_CONF = "/etc/zabbix/zabbix_agentd.conf"
CONF_IGNORE_ITEM = [
    "PidFile",
    "StartAgents"
]
# EOF CONFIG

def init_logger(level, logfile=None):
    """日志功能初始化。
    如果使用日志文件记录，那么则默认使用 RotatinFileHandler 的大小轮询方式，
    默认每个最大 10 MB，最多保留 5 个。
    Args:
        level: 设定的最低日志级别。
        logfile: 设置日志文件路径，如果不设置则表示将日志输出于标准输出。
    """
    import os
    import sys
    if not logfile:
        logging.basicConfig(
            level = getattr(logging, level.upper()),
            format = "%(asctime)s [%(levelname)s] %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S"
        )
    else:
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, level.upper()))
        if logfile.lower() == "local":
            logfile = os.path.join(sys.path[0], os.path.basename(os.path.splitext(__file__)[0]) + ".log")
        handler = RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logging.info("Logger init finished.")

def info_echo(panel, obj):
    s = ""
    if isinstance(obj, (list,)):
        s = "".join(obj)
    else:
        s = str(obj)
    print("="*10 + " {!s} ".format(panel) + "="*10)
    print(s)
    print("="*10 + " {!s} ".format("EOF") + "="*10)

def lnx_command_execute(command_lst):
    """在 Linux 平台执行命令。

    Args:
        command_lst: 命令列表，shell 下命令的空格分段形式。
    Returns:
        <bool> False: 执行返回非预期 exitcode。
        <bool> True: 执行返回预期 exitcode。
    """
    logging.info("---------- {!s} ----------".format(command_lst))
    try:
        res = subprocess.check_output(command_lst, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        for i in e.output.split('\n'):
            logging.error(i)
        logging.info("-"*30)
        return False
    for i in [i for i in res.split('\n') if not i.strip()]:
        logging.info(i)
    logging.info("-"*30)
    return True

def parse_zbx_conf(path):
    """分析 zabbix-agentd 和 zabbix-agent 等的 conf 文件，转为换列表。

    Args:
        path: 配置文件路径。
    Returns:
        <list>: 配置文件的键值对。
    """
    from ConfigParser import RawConfigParser
    from StringIO import StringIO

    cp = RawConfigParser()
    cp.optionxform = str
    with open(path, "r") as f:
        stream = StringIO("[dummy_section]\n" + f.read())
        cp.readfp(stream)
    return cp.items("dummy_section")

def update_diff_conf(path, update_items, add_items, ignore_items):
    """将差异的 conf 文件内容补齐。

    Args:
        path: 需补齐的配置文件路径。
        update_items: 需更新的补齐。
        add_items: 需增加的补齐。
    """
    line_list = []
    with open(path, "r") as f:
        original_list = [l for l in f]
    tmp_line_list = [l for l in original_list]

    logging.debug("start to deal with update_items ......")
    for l in tmp_line_list:
        line = l
        # print(l, end="")
        for i in update_items:
            if i[0] in ignore_items:
                continue
            tmp_re = re.search(r"^\s*{!s}\s*=\s*(.*?)\s*$".format(i[0]), l)
            if tmp_re:
                logging.info("update items on agnet2: {!s} = {!s} -> {!s}".format(
                    i[0],
                    tmp_re.group(1),
                    i[1]))
                line = "{!s} = {!s}\n".format(i[0], i[1])
                break
        line_list.append(line)

    logging.debug("start to deal with add_items ......")
    for i in add_items:
        if i[0] in ignore_items:
            continue
        i_idx = -1
        for l_idx in range(len(line_list)):
            if re.search(r"^\s*#\s*{!s}\s*=.*?$".format(i[0]), line_list[l_idx]):
                i_idx = l_idx
                break
        if i_idx < 0:
            logging.info("for item {!s}, append for tail".format(i[0]))
            if line_list[-1][-1] == "\n":
                line_list.append("{!s} = {!s}\n".format(i[0], i[1]))
            else:
                line_list.append("\n{!s} = {!s}\n".format(i[0], i[1]))
        else:
            logging.info("for item {!s}, insert to line {!s}".format(i[0], str(i_idx+1)))
            line_list = line_list[:i_idx+1] + ["{!s} = {!s}\n".format(i[0], i[1]),] + line_list[i_idx+1:]

    # bacup(options)
    path_bak = path + ".agent2upgrade.bak"
    with open(path_bak, "w") as f:
        for line in original_list:
            f.write(line)

    with open(path, "w") as f:
        for line in line_list:
            f.write(line)

def systemctl_action(action, service):
    """
    """
    if action not in ("start", "stop", "restart", "enable", "disable"):
        raise Exception("not support the systemctl action: {!s}".format(action))
    command_lst = ["systemctl", action, service]
    if lnx_command_execute(command_lst):
        logging.debug("systemctl action is successful")
        return True
    else:
        logging.debug("systemctl action is failed")
        return False

def get_sysversion():
    """获取当前操作系统的版本信息。

    Returns:
        <str> "win": 所有 windows 平台。
        <str> "el5": CentOS/RedHat 5。
        <str> "el6": CentOS/RedHat 6。
        <str> "el7": CentOS/RedHat 7。
    """
    if platform.system().lower() == "windows":
        return "win"
    elif platform.system().lower() == "linux":
        res_tmp = subprocess.check_output(["uname", "-r"]).strip()
        res = re.search('el[0-9]', res_tmp).group()
        if res:
            return res
        else:
            logging.error("Cannot get sysversion from [{!s}].".format(res_tmp))
            raise Exception()

def upgrade_pre(is_force=False):
    """升级前的检查和信息反馈。
    """
    # only support el7 OS
    os_version = get_sysversion()
    if os_version != "el7":
        raise Exception("not support the os version")
    logging.info("the os version is {!s}".format(os_version))

    # check agent2 is installed
    if os.path.isfile(AGENT2_PATH) and not is_force:
        raise Exception("the agent2 has been installed on {!s}".format(_agent2_path))

    # echo current agentd version
    if os.path.isfile(AGENTD_PATH):
        command_lst = [AGENTD_PATH, "--version"]
        pipe = subprocess.Popen(command_lst, stdout=subprocess.PIPE)
        info_echo("version", pipe.stdout.read().decode("utf-8").strip())

def install_agent2_rpm(url):
    """安装 agnet2 的 rpm 包。
    """
    command_lst = ["rpm", "-ivh", url]
    if lnx_command_execute(command_lst):
        logging.info("zabbix-agent2 rpm is installed successfully")
    else:
        logging.error("zabbix-agent2 rpm installing is failed")
        raise Exception()

def conv_agent2_conf(agentd_conf_path, agent2_conf_path):
    """对齐存在的 agentd 的配置。
    """
    agentd_items = parse_zbx_conf(agentd_conf_path)
    agent2_items = parse_zbx_conf(agent2_conf_path)
    diff_set = set()
    add_set = set()
    for i in agentd_items:
        has_found = False
        is_diff = False
        for j in agent2_items:
            if i[0] == j[0]:
                has_found = True
                if i[1] != j[1]:
                    is_diff = True
        if not has_found:
            add_set.add(i[0])
        else:
            if is_diff:
                diff_set.add(i[0])

    if len(diff_set) == 0 and len(add_set) == 0:
        return 

    update_items = [(i[0], i[1]) for i in agentd_items if i[0] in diff_set]
    add_items = [(i[0], i[1]) for i in agentd_items if i[0] in add_set]
    logging.debug("in conv_agent2_conf, update_items: {!s}".format(str(update_items)))
    logging.debug("in conv_agent2_conf, add_items: {!s}".format(str(add_items)))
    update_diff_conf(AGENT2_CONF, update_items, add_items, CONF_IGNORE_ITEM)

def conv_agent2_enable():
    """
    """
    if not systemctl_action("stop", "zabbix-agent"):
        logging.error("systemctl stop zabbix-agent is failed, please check")
        return False
    if not systemctl_action("disable", "zabbix-agent"):
        logging.error("systemctl disable zabbix-agent is failed, please check")
        return False
    if not systemctl_action("start", "zabbix-agent2"):
        logging.error("systemctl start zabbix-agent2 is failed, please check")
        return False
    if not systemctl_action("enable", "zabbix-agent2"):
        logging.error("systemctl enable zabbix-agent2 is failed, please check")
        return False
    return True

def execute(url):
    # 1. 抓取一次当前 agentd 的版本，备份 agentd 的文件。
    upgrade_pre()
    # 2. yum/rpm 安装对应的 agent2 rpm。
    install_agent2_rpm(url)
    # 3. 根据现有的 agentd 的配置填充到 agent2 中。
    conv_agent2_conf(AGENTD_CONF, AGENT2_CONF)
    # 4. systemctl stop zabbix-agent 或 service zabbix-agent stop。（这里最好 rhel7 的才升级）
    # systemctl disable zabbix-agent
    # systemctl start zabbix-agent2
    # systemctl enable zabbix-agent2
    conv_agent2_enable()


if __name__ == "__main__":
    # ########## Self Test
    INPUT_AGENT2_RPM_URL = "http://192.168.66.180:8080/zabbix-agent2-5.0.1-1.el7.x86_64.rpm"
    # ########## EOF Self Tes

    init_logger("debug")
    try:
        execute(
            url = INPUT_AGENT2_RPM_URL
        )
    except Exception as e:
        logging.exception(e)
        exit(1)