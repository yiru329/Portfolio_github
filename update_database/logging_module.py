# _*_ coding:utf-8   _*_
import logging

__author__ = 'yara'

def set_logging(profile):
    # 实例化一个logger对象
    logger = logging.getLogger()
    # 设置初始显示级别
    logger.setLevel(logging.DEBUG)

    # 创建一个文件句柄
    file_handle = logging.FileHandler("{}.log".format(profile), encoding="UTF-8")

    # 创建一个流句柄
    # stream_handle = logging.StreamHandler()

    # 创建一个输出格式
    fmt = logging.Formatter(f"{'*' * 28}\n"
                            "> %(asctime)s\n"
                            "> %(levelname)s - "
                            "%(filename)s - "
                            "[line:%(lineno)d]\n"
                            f"{'-' * 40}\n"
                            "  %(message)s\n"
                            f"{'-' * 40}\n\n",
                            datefmt="%a, %d %b %Y "
                                    "%H:%M:%S"
                            )
    # 文件句柄设置格式
    file_handle.setFormatter(fmt)
    # logger对象绑定文件句柄
    logger.addHandler(file_handle)
