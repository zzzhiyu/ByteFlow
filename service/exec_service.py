import datetime
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from ds.crud.data_source import select_data_sources


def run_datax_task(file_name, datax_json):
    # 创建临时文件
    tmp_dir = os.path.dirname(os.path.realpath(__file__))
    with tempfile.NamedTemporaryFile(mode='w+', prefix=file_name, suffix='.json', dir=tmp_dir) as tmp_fp:
        print(f'临时目录为:{tmp_fp.name}')
        # 内容写入临时文件中
        tmp_json = json.dumps(datax_json, indent=4, separators=(',', ':'))
        tmp_fp.write(tmp_json)
        tmp_fp.flush()
        tmp_fp.seek(0)
        # 执行改json文件
        shell = f'datax.py {tmp_fp.name}'
        code, output = subprocess.getstatusoutput(shell)
    print('datax执行结果为: \n' + output)
    sys.stdout.flush()
    if code != 0:
        print('任务执行失败:\n' + datax_json)
        raise Exception('任务执行失败:\n' + datax_json)


def run_datax_job(table_name: str, task_json: dict):
    # 得到task_json的基本信息
    source_name = task_json['source_name']
    db_type = task_json['db_type']
    parallel = task_json['parallel']
    datax_json = task_json['datax_json']
    try:
        # 得到库信息
        data_sources = select_data_sources(source_name, db_type)
        read_conn = datax_json['job']['content'][0]['reader']['parameter']['connection']
        query_sql = read_conn[0]['querySql'][0]
        jdbc_conn = read_conn[0]['jdbcUrl'][0]
        # 初始化变量
        index = 0
        new_read_conns = []
        for data_source in data_sources:
            # 获取连接信息
            new_query_sql = query_sql.replace('${server_id}', str(data_source.server_id))
            new_jdbc_conn = (jdbc_conn.replace('{host}', data_source.host).replace('{port}', str(data_source.port))
                             .replace('{db_name}', data_source.db_name))
            new_read_conn = {'querySql': [new_query_sql], 'jdbcUrl': [new_jdbc_conn]}
            new_read_conns.append(new_read_conn)
            index += 1
            if index >= parallel:
                # 更新connection
                datax_json['job']['content'][0]['reader']['parameter']['connection'] = new_read_conns
                run_datax_task(table_name, datax_json)
                new_read_conns.clear()
                index = 0
        if new_read_conns:
            datax_json['job']['content'][0]['reader']['parameter']['connection'] = new_read_conns
            run_datax_task(table_name, datax_json)
        print(f"\n\n\ndatax 数据拉取完成\n\n\n")
    except Exception as err:
        now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"\n\n\n{now}日期的数据拉取失败, 请重新拉取\n\n\n")
        raise Exception(f"执行datax任务失败: {err.__str__()}")


def execute():
    if len(sys.argv) != 2:
        print('输入参数错误，请输入要读取的文件名!!!')
        raise Exception('输入参数错误，请输入要读取的文件名!!!')
    task_json_path = sys.argv[1]
    table_name = Path(task_json_path).name.split(".")[0]
    # 读取Json内容
    with open(task_json_path) as fp:
        task_json = json.load(fp)
    run_datax_job(table_name, task_json)


if __name__ == '__main__':
    execute()
