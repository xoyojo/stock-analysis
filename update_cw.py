#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
读取通达信专业财务数据文件 /vipdoc/cw/gpcw?????.dat
感谢大神们的研究 https://github.com/rainx/pytdx/issues/133

财务文件无需天天更新，上市公司发了季报后财务文件才会更新，因此更新大概率集中在财报季。

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""

import os
import csv
import time
import requests
import datetime
import hashlib
import zipfile
import pandas as pd
import pytdx.reader.gbbq_reader
from pathlib import Path

import func
import user_config as ucfg


# 变量定义
starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime = time.time()

# 常量定义
TDX_PATH = Path(ucfg.tdx["tdx_path"])
CW_PATH = TDX_PATH / "vipdoc/cw"
CSV_CW_PATH = Path(ucfg.tdx["csv_cw"])
CSV_GBBQ_PATH = Path(ucfg.tdx["csv_gbbq"])

# 创建必要的目录
CSV_CW_PATH.mkdir(parents=True, exist_ok=True)
CSV_GBBQ_PATH.mkdir(parents=True, exist_ok=True)

# 主程序开始


# 本机专业财务文件和通达信服务器对比，检查更新

# 下载通达信服务器文件校检信息txt
tdx_gpcw_df = pd.DataFrame(
    [
        l.strip().split(",")
        for l in func.download_url(ucfg.tdx["gpcw_url"]).text.strip().split("\r\n")
    ],
    columns=["filename", "md5", "filesize"],
)

# 检查本机通达信dat文件是否有缺失

local_zipfile_list = func.list_localTDX_cwfile("zip")  # 获取本机已有文件
many_thread_download = func.ManyThreadDownload()
for df_filename in tdx_gpcw_df["filename"].tolist():
    starttime_tick = time.time()
    if df_filename not in local_zipfile_list:
        print(f"{df_filename} 本机没有 开始下载")
        tdx_zipfile_url = "http://down.tdx.com.cn:8001/tdxfin/" + df_filename
        local_zipfile_path = (
            ucfg.tdx["tdx_path"]
            + os.sep
            + "vipdoc"
            + os.sep
            + "cw"
            + os.sep
            + df_filename
        )
        many_thread_download.run(tdx_zipfile_url, local_zipfile_path)
        with zipfile.ZipFile(
            local_zipfile_path, "r"
        ) as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
            zipobj.extractall(ucfg.tdx["tdx_path"] + os.sep + "vipdoc" + os.sep + "cw")
        local_datfile_path = local_zipfile_path[:-4] + ".dat"
        df = func.historyfinancialreader(local_datfile_path)
        csvpath = ucfg.tdx["csv_cw"] + os.sep + df_filename[:-4] + ".pkl"
        df.to_pickle(csvpath, compression=None)
        print(f"{df_filename} 完成更新 用时 {(time.time() - starttime_tick):>5.2f} 秒")

# 检查本机通达信zip文件是否需要更新
local_zipfile_list = func.list_localTDX_cwfile("zip")  # 获取本机已有文件
for zipfile_filename in local_zipfile_list:
    starttime_tick = time.time()
    local_zipfile_path = (
        ucfg.tdx["tdx_path"]
        + os.sep
        + "vipdoc"
        + os.sep
        + "cw"
        + os.sep
        + zipfile_filename
    )
    with open(local_zipfile_path, "rb") as fobj:  # 读取本机zip文件，计算md5
        file_content = fobj.read()
        file_md5 = hashlib.md5(file_content).hexdigest()
    if file_md5 not in tdx_gpcw_df["md5"].tolist():  # 本机zip文件的md5与服务器端不一致
        print(f"{zipfile_filename} 需要更新 开始下载")
        os.remove(local_zipfile_path)  # 删除本机zip文件
        tdx_zipfile_url = "http://down.tdx.com.cn:8001/tdxfin/" + zipfile_filename
        many_thread_download.run(tdx_zipfile_url, local_zipfile_path)
        with zipfile.ZipFile(
            local_zipfile_path, "r"
        ) as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
            try:
                zipobj.extractall(
                    ucfg.tdx["tdx_path"] + os.sep + "vipdoc" + os.sep + "cw"
                )
            except zipfile.BadZipFile:
                os.remove(local_zipfile_path)  # 删除本机zip文件
                print(
                    f"文件{local_zipfile_path}下载损坏，或服务器端文件错误，跳过此文件"
                )

        local_datfile_path = local_zipfile_path[:-4] + ".dat"
        df = func.historyfinancialreader(local_datfile_path)
        csvpath = ucfg.tdx["csv_cw"] + os.sep + zipfile_filename[:-4] + ".pkl"
        df.to_pickle(csvpath, compression=None)
        print(
            f"{zipfile_filename} 完成更新 用时 {(time.time() - starttime_tick):>5.2f} 秒"
        )

# 检查本机财报导出文件是否存在
cwfile_list = os.listdir(ucfg.tdx["csv_cw"])  # cw目录 生成文件名列表
local_datfile_list = func.list_localTDX_cwfile("dat")  # 获取本机已有文件
for filename in local_datfile_list:
    starttime_tick = time.time()
    filenamepkl = filename[:-4] + ".pkl"
    pklpath = ucfg.tdx["csv_cw"] + os.sep + filenamepkl
    filenamedat = filename[:-4] + ".dat"
    datpath = (
        ucfg.tdx["tdx_path"] + os.sep + "vipdoc" + os.sep + "cw" + os.sep + filenamedat
    )
    if filenamepkl not in cwfile_list:  # 本机zip文件的md5与服务器端不一致
        print(f"{filename} 本机没有 需要导出")
        df = func.historyfinancialreader(datpath)
        df.to_pickle(pklpath, compression=None)
        print(f"{filename} 完成更新 用时 {(time.time() - starttime_tick):>5.2f} 秒")

print(f"专业财务文件检查更新完成 已用 {(time.time() - starttime):>5.2f} 秒")

# 解密通达信股本变迁文件
starttime_tick = time.time()
category = {
    "1": "除权除息",
    "2": "送配股上市",
    "3": "非流通股上市",
    "4": "未知股本变动",
    "5": "股本变化",
    "6": "增发新股",
    "7": "股份回购",
    "8": "增发新股上市",
    "9": "转配股上市",
    "10": "可转债上市",
    "11": "扩缩股",
    "12": "非流通股缩股",
    "13": "送认购权证",
    "14": "送认沽权证",
}
print(f"解密通达信gbbq股本变迁文件")
filepath = ucfg.tdx["tdx_path"] + "/T0002/hq_cache/gbbq"
df_gbbq = pytdx.reader.gbbq_reader.GbbqReader().get_df(filepath)
df_gbbq.drop(columns=["market"], inplace=True)
df_gbbq.columns = [
    "code",
    "权息日",
    "类别",
    "分红-前流通盘",
    "配股价-前总股本",
    "送转股-后流通盘",
    "配股-后总股本",
]
df_gbbq["类别"] = df_gbbq["类别"].astype("object")
df_gbbq["code"] = df_gbbq["code"].astype("object")
for i in range(df_gbbq.shape[0]):
    df_gbbq.iat[i, df_gbbq.columns.get_loc("类别")] = category[
        str(df_gbbq.iat[i, df_gbbq.columns.get_loc("类别")])
    ]
df_gbbq.to_csv(ucfg.tdx["csv_gbbq"] + os.sep + "gbbq.csv", encoding="gbk", index=False)
# 如果读取，使用下行命令
# df_gbbq = pd.read_csv(ucfg.tdx['csv_cw'] + '/gbbq.csv', encoding='gbk', dtype={'code': 'object'})
print(f"股本变迁解密完成 用时 {(time.time() - starttime_tick):>5.2f} 秒")
print(f"全部完成 用时 {(time.time() - starttime):>5.2f} 秒 程序结束")
