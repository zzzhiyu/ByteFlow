# 元数据信息
host = '{host}'
port = -1
user = '{user}'
passwd = '{passwd}'
db = 'byte_flow'


#doris库的信息
doris_load_url = ['{host}:{port}', '{host}:{port}', '{host}:{port}']
doris_host = '{host}'
doris_port = -1
doris_user = '{user}'
doris_passwd = '{passwd}'
doris_flush_interval = 30000
doris_jdbc_url = 'jdbc:mysql://{host}:{port}/{db_name}'
doris_db = '{db_name}'


# 支持目的库信息
dst_engine_list = ['doris']


# 作业提交到dolphinscheduler的参数
# 需要在资源中心创建该目录并把service文件里的内容导入项目中
dolps_build_job_exec = 'byte_flow/service/exec_service.py'

dolps_username = '{username}' # 按照实际情况填写
dolps_warning_group_id = 6   # 按照实际情况填写
dolps_timezone = 'Asia/Shanghai'
dolps_worker_group = 'default'
dolps_warning_type = 'FAILURE'
dolps_execution_type = 'PARALLEL'
dolps_release_state = 'online'

# 需要建立每日增量/全量拉取项目,名称按照实际情况填写, 增量/全量拉取项目
dolps_project = '{project_name}'
# 资源中心需要建立相应的目录，存放相应的json脚本
dolps_json_dir = '{project_name}/'

# 需要建立历史全量同步项目,名称按照实际情况填写, 全量拉取历史数据拉取项目
dolps_full_project = '{his_project_name}'
# 资源中心需要建立相应的目录，存放相应的json脚本
dolps_full_json_dir = '{his_project_name}/'