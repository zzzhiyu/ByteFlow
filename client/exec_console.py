import os
import sys

from conf import config
from datax.datax_task import DataxTask
from dolphinscheduler.workflow import DolpsWorkflow
from model.data_source_infos import DataSourceInfos
from ds.crud.data_source import select_data_sources, select_distinct_db_types, select_distinct_source_names
from reader.reader import Reader
from writer.writer import Writer
from util import console

sys.path.append(os.path.dirname(os.path.realpath(__file__)))


def execute_jdbc_migration(dst_engine: str):
    reader = None
    writer = None
    data_source_infos = None
    select_func = "2"  # 默认为第二模式, 输入source_name和db_type

    try:
        # 提前连接writer
        writer = Writer.create_writer(dst_engine)
        writer.open()

        while True:
            if select_func == "2":
                # 先关闭reader
                if reader:
                    reader.close()
                # 确定source_name, db_type
                source_name = console.input_value_and_check_contain("请输入需要采集游戏的source_name", select_distinct_source_names())
                db_type = console.input_value_and_check_contain("请输入源库db_type类型", select_distinct_db_types())
                # 获取某一特定的data_source(假如data_sources里有多库多表就取多库)
                data_source_infos = DataSourceInfos(select_data_sources(source_name, db_type))
                # 连接reader
                reader = Reader.create_reader(data_source_infos)
                reader.open()

            # 获取reader_table_info信息和一库多服务信息
            reader_table_info = reader.get_table_info()
            print(f"\n{reader_table_info.table_name}表: 占用的存储空间约为 {reader_table_info.get_storage_size_gb()} G. "
                  f"分库有 {data_source_infos.num}个, 与北京时间慢 {reader_table_info.interval_unit} 时!!!\n")

            # 初始化writer的成员变量
            writer.set_field(reader_table_info)
            # 获取writer table_info并建立相关表
            writer_table_info = writer.get_table_infos()
            writer.create_table(writer_table_info)

            # DataxTask类生成
            datax_task = DataxTask(reader_table_info, writer_table_info, reader.src_engine, writer.dst_engine,
                                   data_source_infos.num)

            # DolpsWorkflow类生成
            dolsp_workflow = DolpsWorkflow(writer_table_info.table_name, reader.src_engine, writer.dst_engine,
                                           reader_table_info.interval_unit, writer.is_auto_input)

            # 提交到dolphinscheduler,增量进行提交
            json_file_path = config.dolps_json_dir + writer_table_info.table_name + '.json'
            incr_task_content = datax_task.get_datax_task(False)
            dolsp_workflow.submit_incr_fetch_workflow(json_file_path, incr_task_content)

            # 提交到dolphinscheduler,全量进行提交
            full_json_file_path = config.dolps_full_json_dir + writer_table_info.table_name + '.json'
            full_task_content = datax_task.get_datax_task(True)
            dolsp_workflow.run_full_fetch_workflow(full_json_file_path, full_task_content)

            # 进行选择
            select_func = console.input_value_and_check_contain("1.不修改gametype, dbtype继续配置表. 2.修改gametype, dbtype"
                                                                "再配置表. 3.退出,重新选择数据源  请选择功能", ["1", "2", "3"])
            if select_func == "3":
                break
    finally:
        if reader:
            reader.close()
        if writer:
            writer.close()


def exec_console():
    while True:
        console.print_select_box("1.关系型数据(JDBC)集成     2.退出")
        func_module = console.input_value_and_check_contain("请选择功能模块", ["1", "2"])
        if func_module == "1":
            if len(config.dst_engine_list) == 1:
                dst_engine = config.dst_engine_list[0]
            else:
                dst_engine = console.input_value_and_check_contain("请选择要写入数据的目的引擎", config.dst_engine_list)
            execute_jdbc_migration(dst_engine)
        else:
            print("程序退出...\n")
            break


if __name__ == '__main__':
    print("ByteFlow Starting...\n")
    exec_console()
