import ast
import os.path

class DynamicClass(object):
    def __init__(self, module_path: str, class_name: str):
        self.module_path = module_path
        self.class_name = class_name

    @classmethod
    def load_class_info(cls, full_parent_dir: str, parent_module: str, infer_class_name: str) -> 'DynamicClass':
        """
        获取目的的类名称，以及相关的module和classname
        :param full_parent_dir: 解析目录下的所有文件
        :param parent_module: 该目录所属的module
        :param infer_class_name:  查找的类名字
        :return: 相关的类信息
        """
        module_path = None
        class_name = None
        # 遍历目录下的文件
        for filename in os.listdir(full_parent_dir):
            if filename.endswith(".py"):
                with open(f"{full_parent_dir}/{filename}", "rb") as f:
                    code_content = f.read()
                ast_tree = ast.parse(code_content)
                for node in ast.walk(ast_tree):
                    # 找到对应的类名
                    if isinstance(node, ast.ClassDef) and infer_class_name.lower() == node.name.lower():
                        class_name = node.name
                        module_path = f"{parent_module}.{filename[:-3]}"
                        break
                if class_name:
                    break
        return DynamicClass(module_path, class_name)

