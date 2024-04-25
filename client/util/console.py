from typing import List


def print_select_box(print_info: str):
    """
    打印选择框
    :param print_info: 需要打印的信息
    :return:
    """
    handle_info = f"     {print_info}     "
    print(f"\n+{'-' * (len(handle_info.encode('utf-8')) - 2)}+")
    print(handle_info)
    print(f"+{'-' * (len(handle_info.encode('utf-8')) - 2)}+")


def input_value(prompt_msg: str) -> str:
    """
    提示输入框: 输入字符串
    :param prompt_msg: 提示信息
    :return:
    """
    if prompt_msg[-2:] != ":\n":
        prompt_msg += ":\n"
    input_value = input(prompt_msg).lower()
    while not input_value:
        input_value = input(f"输入错误,请重新输入!!!{prompt_msg}").lower()
    return input_value


def input_value_and_check_bool(prompt_msg: str) -> bool:
    """
    提示输入框: 输入Y/y/N/n
    :param prompt_msg: 提示信息
    :return:
    """
    if prompt_msg[-2:] == ":\n":
        prompt_msg = prompt_msg[:-2]
    if "[y/Y/n/N]" not in prompt_msg:
        prompt_msg += " [y/Y/n/N]"
    prompt_msg += ":\n"
    input_value = input(prompt_msg).lower()
    while input_value not in ["y", "n"]:
        input_value = input(f"输入错误,请重新输入!!!{prompt_msg}").lower()
    return input_value == "y"


def input_value_and_check_positive_integer(prompt_msg: str) -> int:
    """
    提示输入框: 输入正整数
    :param prompt_msg: 提示信息
    :return:
    """
    if prompt_msg[-2:] == ":\n":
        prompt_msg = prompt_msg[:-2]
    prompt_msg += " [请输入正整数]:\n"
    input_value = input(prompt_msg)
    while not input_value.isdigit():
        input_value = input(f"输入错误,请重新输入!!!{prompt_msg}")
    return int(input_value)


def input_value_and_check_contain(prompt_msg: str, constrain_values: List[str]) -> str:
    """
    提示输入框: 输入包含的内容, 可以输入一个值
    :param prompt_msg: 提示信息
    :param constrain_values: 约束条件
    :return:
    """
    check_info = f"[{','.join(constrain_values)}]"
    if prompt_msg[-2:] == ":\n":
        prompt_msg = prompt_msg[:-2]
    if check_info not in prompt_msg:
        prompt_msg += f" {check_info}"
    prompt_msg += ":\n"
    input_value = input(prompt_msg).lower()
    while input_value not in constrain_values:
        input_value = input(f"输入错误,请重新输入!!!{prompt_msg}").lower()
    return input_value


def input_values_and_check_contain(prompt_msg: str, constrain_values: List[str]) -> List[str]:
    """
    提示输入框: 输入包含的数字, 可以输入多个值
    :param prompt_msg: 提示信息
    :param constrain_values: 约束条件
    :return:
    """
    if prompt_msg[-2:] == ":\n":
        prompt_msg = prompt_msg[:-2]
    prompt_msg += "[注:可以输入多个值(不区分大小写), 用 ',' 分开]:\n"
    input_values = input(prompt_msg).lower()
    result_values = []
    index = 0
    while index == 0:
        for value in input_values.split(","):
            if value in constrain_values and value not in result_values:
                result_values.append(value)
                index += 1
            else:
                index = 0
                result_values.clear()
                input_values = input(f"输入错误,请重新输入!!!{prompt_msg}").lower()
                break
    return result_values
