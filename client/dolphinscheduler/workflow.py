import re
import time
import datetime

from pydolphinscheduler.core import Workflow
from pydolphinscheduler.core.resource import Resource
from pydolphinscheduler.tasks import Shell

from conf import config
from util import console


class DolpsWorkflow(object):

    def __init__(self, dst_table_name: str, src_engine: str, dst_engine: str, interval_time: int, is_auto_input: bool):
        self.dst_table_name = dst_table_name
        self.src_engine = src_engine
        self.dst_engine = dst_engine
        self.interval_time = interval_time
        self.is_auto_input = is_auto_input

    def _set_exec_time(self) -> str:
        if self.dst_table_name.split("_")[-1] in ["di", "df"]:
            if self.is_auto_input:
                hour = int(time.time()) % 2 + self.interval_time
                minute = int(time.time()) % 60
                exec_time = f"{hour}:{minute}"
            else:
                exec_time = input(f'请输入任务每天执行的时间(拉取时间需要在 {self.interval_time}:00以后) 如00:30 ,1:30, 15:21 :\n')
                time_search = re.search('[0-2]?[0-9]:[0-6][0-9]', exec_time)
                while (time_search is None
                       or time_search.end() - time_search.start() != len(exec_time)
                       or int(exec_time.split(":")[0]) < self.interval_time):
                    exec_time = input('输入错误!!!请输入任务每天执行的时间 如1:30, 15:21: \n')
                    time_search = re.search('[0-2]?[0-9]:[0-6][0-9]', exec_time)
            exec_time = f"0{exec_time}" if len(exec_time) == 4 else exec_time
        elif self.dst_table_name.split("_")[-1] in ["ri", "rf"]:
            minute = 10
            if not self.is_auto_input:
                minute = console.input_value_and_check_positive_integer('请输入每隔几分钟拉取一次数据[例如: 10]:\n')
            start_minute = int(time.time()) % minute
            exec_time = f"{start_minute}/{minute}"
        else:
            raise Exception("只支持 ri,rf,di,df表")
        return exec_time

    def _get_scheduler(self, exec_time: str):
        if self.dst_table_name.split("_")[-1] in ["ri", "rf"]:
            scheduler = f'0 {exec_time} * * * ? *'
        elif self.dst_table_name.split("_")[-1] in ["di", "df"]:
            hour, minute = exec_time.split(":")
            scheduler = f"0 {int(minute)} {int(hour)} * * ? *"
        else:
            raise Exception("只支持 ri,rf,di,df表")
        return scheduler

    def _get_timeout(self, exec_time: str) -> int:
        if self.dst_table_name.split("_")[-1] in ["ri", "rf"]:
            timeout = int(exec_time.split("/")[1])
        else:
            timeout = 1200
        return timeout

    def submit_incr_fetch_workflow(self, json_file_path: str, content: str):
        """提交增量拉取任务任务"""
        exec_time = self._set_exec_time()
        schedule = self._get_scheduler(exec_time)
        timeout = self._get_timeout(exec_time)
        with Workflow(
                name=f"{self.dst_table_name}_{exec_time}",
                description=f"{self.src_engine} to {self.dst_engine}",
                schedule=schedule,
                online_schedule=True,
                timezone=config.dolps_timezone,
                user=config.dolps_username,
                project=config.dolps_project,
                worker_group=config.dolps_worker_group,
                warning_type=config.dolps_warning_type,
                warning_group_id=config.dolps_warning_group_id,
                execution_type=config.dolps_execution_type,
                timeout=timeout,
                release_state=config.dolps_release_state,
                # json文件提交到dolphinscheduler
                resource_list=[
                    Resource(name=json_file_path, content=content, user_name=config.dolps_username,
                             description=f"{self.src_engine} to {self.dst_engine}")],
        ) as workflow:
            # 确定shell执行的脚本
            command = f"python3 {config.dolps_build_job_exec} '{json_file_path}'"
            task_name = self.dst_table_name + '_task'
            task = Shell(name=task_name, command=command, resource_list=[config.dolps_build_job_exec, json_file_path])
            task.timeout = datetime.timedelta(minutes=timeout)
            task.timeout_notify_strategy = 'WARNFAILED'
            workflow.submit()
        print(f'提交dolphinscheduler工作流成功!任务josn文件存放位置为:{json_file_path}, project名字为:{config.dolps_project}, '
              f'workflow_name 为: {self.dst_table_name}_{exec_time}\n\n')

    def run_full_fetch_workflow(self, json_file_path: str, content: str):
        """提交全量拉取任务任务"""
        # 不是自动拉取，判断是否进行全量数据拉取
        if not self.is_auto_input and not console.input_value_and_check_bool("是否拉取全量数据"):
            return
        with Workflow(
                name=self.dst_table_name,
                description=f"{self.src_engine} to {self.dst_engine}",
                online_schedule=False,
                timezone=config.dolps_timezone,
                user=config.dolps_username,
                project=config.dolps_full_project,
                worker_group=config.dolps_worker_group,
                warning_type=config.dolps_warning_type,
                warning_group_id=config.dolps_warning_group_id,
                execution_type=config.dolps_execution_type,
                release_state=config.dolps_release_state,
                # json文件提交到dolphinscheduler
                resource_list=[
                    Resource(name=json_file_path, content=content, user_name=config.dolps_username,
                             description=f"{self.src_engine} to {self.dst_engine}")]
        ) as workflow:
            # 确定shell执行的脚本
            command = f"python3 {config.dolps_build_job_exec} '{json_file_path}'"
            task_name = self.dst_table_name + '_task'
            task = Shell(name=task_name, command=command, resource_list=[config.dolps_build_job_exec, json_file_path])
            workflow.run()
        print(f'提交dolphinscheduler工作流成功!任务josn文件存放位置为:{json_file_path}, project名字为:{config.dolps_full_project}, '
              f'workflow_name 为: {self.dst_table_name}\n\n')
