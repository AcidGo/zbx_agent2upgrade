# -*- coding: utf-8 -*-


# Author: AcidGo
# Usage:
#   url: 指定安装包的资源路径。


from __future__ import print_function
import platform, glob, os, sys, time
import logging
import shutil
import re
import subprocess

from collections import OrderedDict
from ConfigParser import RawConfigParser
from StringIO import StringIO


# CONFIG
AGENT2_PATH = "/usr/sbin/zabbix_agent2"
AGENT2_CONF = "/etc/zabbix/zabbix_agent2.conf"
AGENTD_PATH = "/usr/sbin/zabbix_agentd"
AGENTD_CONF = "/etc/zabbix/zabbix_agentd.conf"
CONF_IGNORE_ITEM = [
    "PidFile",
    "StartAgents",
]
# from https://www.zabbix.com/documentation/current/manual/config/items/plugins (5.2)
CONF_CONFLICT_UP = [
    "agent.hostname", "agent.ping", "agent.version", 
    "ceph.df.details", "ceph.osd.stats", "ceph.osd.discovery", "ceph.osd.dump", "ceph.ping", "ceph.pool.discovery", "ceph.status", 
    "system.cpu.discovery", "system.cpu.num", "system.cpu.util", "docker.container_info", "docker.container_stats", "docker.containers", 
    "docker.containers.discovery", "docker.data_usage", "docker.images", "docker.images.discovery", "docker.info", "docker.ping", 
    "vfs.file.cksum", "vfs.file.contents", "vfs.file.exists", "vfs.file.md5sum", "vfs.file.regexp", "vfs.file.regmatch", "vfs.file.size", "vfs.file.time", 
    "kernel.maxfiles", "kernel.maxproc", 
    "log", "log.count", "logrt", "logrt.count", 
    "memcached.ping", "memchached.stats", 
    "modbus.get", 
    "mqtt.get", 
    "mysql.db.discovery", "mysql.db.size", "mysql.get_status_variables", "mysql.ping", "mysql.replication.discovery", "mysql.replication.get_slave_status", "mysql.version", 
    "net.if.collisions", "net.if.discovery", "net.if.in", "net.if.out", "net.if.total", 
    "oracle.diskgroups.stats", "oracle.diskgroups.discovery", "oracle.archive.info", "oracle.archive.discovery", "oracle.cdb.info", "oracle.custom.query", 
    "oracle.datafiles.stats", "oracle.db.discovery", "oracle.fra.stats", "oracle.instance.info", "oracle.pdb.info", "oracle.pdb.discovery", 
    "oracle.pga.stats", "oracle.ping", "oracle.proc.stats", "oracle.redolog.info", "oracle.sga.stats", "oracle.sessions.stats", "oracle.sys.metrics", 
    "oracle.sys.params", "oracle.ts.stats", "oracle.ts.discovery", "oracle.user.info", 
    "pgsql.ping", "pgsql.db.discovery", "pgsql.db.size", "pgsql.db.age", "pgsql.database.bloating_tables", "pgsql.replication_lag.sec", "pgsql.replication_lag.b", 
    "pgsql.replication.count", "pgsql.replication.status", "pgsql.replication.recovery_role", "pgsql.cache.hit", "pgsql.connections", "pgsql.archive", 
    "pgsql.bgwriter", "pgsql.dbstat.sum", "pgsql.dbstat", "pgsql.wal.stat", "pgsql.locks", "pgsql.pgsql.oldest.xid", "pgsql.uptime", 
    "proc.cpu.util", 
    "redis.config", "redis.info", "redis.ping", "redis.slowlog.count", 
    "system.swap.size", "system.run", "systemd.unit.discovery", "systemd.unit.info", 
    "net.tcp.port", "net.udp.service", "net.udp.service.perf", 
    "system.hostname", "system.sw.arch", "system.uname", "system.uptime", 
    "vfs.dev.discovery", "vfs.dev.read", "vfs.dev.write", 
    "web.page.get", "web.page.perf", "web.page.regexp", 
    "net.tcp.listen", "net.udp.listen", 
    "sensor", 
    "system.boottime", "system.cpu.intr", "system.cpu.load", "system.cpu.switches", "system.hw.cpu", "system.hw.macaddr", 
    "system.localtime", "system.sw.os", "system.swap.in", "system.swap.out", 
    "vfs.fs.discovery", 
    "zabbix.stats", 
    "net.dns", "net.dns.record", "net.tcp.service", "net.tcp.service.perf", 
    "proc.mem", "proc.num", 
    "system.hw.chassis", "system.hw.devices", "system.sw.packages", "system.users.num", 
    "vfs.dir.count", "vfs.dir.size", "vfs.fs.get", "vfs.fs.inode", "vfs.fs.size", 
    "vm.memory.size", 
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

def parse_zbx_conf(path, is_multi=False):
    """分析 zabbix-agentd 和 zabbix-agent 等的 conf 文件，转为换列表。

    Args:
        path: 配置文件路径。
    Returns:
        <list>: 配置文件的键值对。
    """
    cp = RawConfigParser() if not is_multi else RawConfigParser(dict_type=MultiOrderedDict)
    cp.optionxform = str
    with open(path, "r") as f:
        stream = StringIO("[dummy_section]\n" + f.read())
        cp.readfp(stream)
    return cp.items("dummy_section")

def check_conflict_up(agent_conf_path):
    """检查是否存在有与 Zabbix-Agent2 内置的 key 冲突的 UP。

    Args:
        agent_conf_path: Zabbix-Agentd/2 的配置文件路径。
    Returns:
        <dict>: {<up_path>: [<key1>, <key2> ...]}
    """
    res = {}
    self_key = "@{!s}".format(agent_conf_path)
    for i in parse_zbx_conf(agent_conf_path):
        if i[0] == "UserParameter":
            if self_key not in res:
                res[self_key] = [i[1].split(",")[0].strip()]
            else:
                res[self_key].append(i[1].split(",")[0].strip())
        if i[0] == "Include":
            for j in glob.glob(i[1]):
                res[j] = []

    for k in res:
        if k == self_key:
            continue
        up_items = parse_zbx_conf(k, True)
        for up in up_items:
            if up[0] != "UserParameter":
                continue
            for entry in up[1].split("\n"):
                res[k].append(entry.split(",")[0].strip())

    for v in res.values():
        for v_i in v:
            if v_i in CONF_CONFLICT_UP:
                return True, res

    return False, res

def deal_conflict_up(up_dict):
    """
    """
    for k, v in up_dict.items():
        has_deal = False
        for v_i in v:
            if has_deal:
                break
            if v_i in CONF_CONFLICT_UP:
                if k.startswith("@"):
                    conf_path = k.split("@")[-1].strip()
                    context_list = []
                    with open(conf_path, "r") as f:
                        original_list = [l for l in f]
                    for line in original_list:
                        if re.search(r"^\s*UserParameter\s*=\s*{!s}\s*,.*?$".format(v_i), line):
                            logging.info("deal with conflict UserParameter on self config: {!s}".format(line.strip()))
                            continue
                        context_list.append(line)
                    with open(conf_path, "w") as f:
                        for line in context_list:
                            f.write(line)
                else:
                    # 直接整个文件关闭匹配
                    conf_dir = os.path.dirname(k)
                    conf_name = os.path.basename(k)
                    disable_path = os.path.join(conf_dir, conf_name + ".disable")
                    shutil.move(k, disable_path)
                    logging.info("deal with conflict UserParameter on outer config: {!s} -> {!s}".format(k, disable_path))
                    has_deal = True
                    break

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
    if action not in ("start", "stop", "restart", "status", "enable", "disable"):
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
        raise Exception("the agent2 has been installed on {!s}".format(AGENT2_PATH))

    # echo current agentd version
    if os.path.isfile(AGENTD_PATH):
        command_lst = [AGENTD_PATH, "--version"]
        pipe = subprocess.Popen(command_lst, stdout=subprocess.PIPE)
        info_echo("version", pipe.stdout.read().decode("utf-8").strip())

def install_agent2_rpm(url, is_force=False):
    """安装 agnet2 的 rpm 包。
    """
    if is_force:
        command_lst = ["yum", "remove", "-y", "zabbix-agent2"]
        if lnx_command_execute(command_lst):
            logging.info("zabbix-agent2 is removed successfully")
        else:
            logging.error("zabbix-agent2 removing is failed")
            raise Exception()

    command_lst = ["rpm", "-ivh", url]
    if lnx_command_execute(command_lst):
        logging.info("zabbix-agent2 rpm is installed successfully")
    else:
        logging.error("zabbix-agent2 rpm installing is failed")
        raise Exception()

def conv_agent2_conf(agentd_conf_path, agent2_conf_path, is_force):
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

    has_conflict, up_dict = check_conflict_up(AGENT2_CONF)

    logging.debug("="*10 + " all UserParameter:")
    for i in up_dict:
        logging.debug("{!s}:".format(i))
        for j in up_dict[i]:
            logging.debug("\t{!s}".format(j))
    logging.debug("="*10 + " EOF all UserParameter")

    if has_conflict:
        logging.warning("found conflict UserParameter in the config")
        if not is_force:
            raise Exception("not force, exit the progress")
        deal_conflict_up(up_dict)

def conv_agent2_enable():
    """
    """
    if os.path.isfile(AGENTD_PATH):
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
    if not systemctl_action("status", "zabbix-agent2"):
        logging.error("systemctl enable zabbix-agent2 is failed, please check")
        return False

    return True

def execute(url, is_force):
    # 1. 抓取一次当前 agentd 的版本，备份 agentd 的文件。
    upgrade_pre(is_force)
    # 2. yum/rpm 安装对应的 agent2 rpm。
    install_agent2_rpm(url, is_force)
    # 3. 根据现有的 agentd 的配置填充到 agent2 中。
    if os.path.isfile(AGENTD_CONF):
        conv_agent2_conf(AGENTD_CONF, AGENT2_CONF, is_force)
    # 4. systemctl stop zabbix-agent 或 service zabbix-agent stop。（这里最好 rhel7 的才升级）
    # systemctl disable zabbix-agent
    # systemctl start zabbix-agent2
    # systemctl enable zabbix-agent2
    # systemctl status zabbix-agent2
    conv_agent2_enable()

class MultiOrderedDict(OrderedDict):
    """from https://stackoverflow.com/questions/15848674/how-to-configparse-a-file-keeping-multiple-values-for-identical-keys
    """
    def __setitem__(self, key, value):
        if isinstance(value, list) and key in self:
            self[key].extend(value)
        else:
            super(MultiOrderedDict, self).__setitem__(key, value)
            # super().__setitem__(key, value) in Python 3


if __name__ == "__main__":
    # ########## Self Test
    INPUT_AGENT2_RPM_URL = "http://192.168.66.180:8080/zabbix-agent2-5.0.1-1.el7.x86_64.rpm"
    INPUT_IS_FORCE = True
    # ########## EOF Self Tes

    init_logger("debug")

    # input args deal
    INPUT_IS_FORCE = True if str(INPUT_IS_FORCE).lower() == "true" else False
    # EOF

    try:
        execute(
            url = INPUT_AGENT2_RPM_URL,
            is_force = INPUT_IS_FORCE
        )
    except Exception as e:
        logging.exception(e)
        exit(1)