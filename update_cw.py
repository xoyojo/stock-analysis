#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
读取通达信专业财务数据文件 /vipdoc/cw/gpcw?????.dat
感谢大神们的研究 https://github.com/rainx/pytdx/issues/133
财务文件无需天天更新，上市公司发了季报后财务文件才会更新，因此更新大概率集中在财报季。
数据单位：金额（元），成交量（股）
"""

import os
from pathlib import Path
import hashlib
import zipfile
import pandas as pd
import pytdx.reader.gbbq_reader

import func
import user_config as ucfg


# 常量定义
TDX_PATH = Path(ucfg.tdx["tdx_path"])
TDX_CW_PATH = TDX_PATH / "vipdoc/cw"
CSV_CW_PATH = Path(ucfg.tdx["csv_cw"])
CSV_GBBQ_PATH = Path(ucfg.tdx["csv_gbbq"])

# 创建必要的目录
CSV_CW_PATH.mkdir(parents=True, exist_ok=True)
CSV_GBBQ_PATH.mkdir(parents=True, exist_ok=True)

many_thread_download = func.ManyThreadDownload()


def update_tdx_cw(filename):
    """
    下载并更新通达信专业财务文件
    """
    url = "http://down.tdx.com.cn:8001/tdxfin/" + filename
    local_zipfile_path = TDX_CW_PATH / filename
    many_thread_download.run(url, local_zipfile_path)
    with zipfile.ZipFile(
        local_zipfile_path, "r"
    ) as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
        try:
            zipobj.extractall(TDX_CW_PATH)
        except zipfile.BadZipFile:
            os.remove(local_zipfile_path)  # 删除本机zip文件
            print(f"文件{local_zipfile_path}下载损坏，或服务器端文件错误，跳过此文件")
    df = func.historyfinancialreader(str(local_zipfile_path)[:-4] + ".dat")
    csvpath = CSV_CW_PATH / (filename[:-4] + ".pkl")
    df.to_pickle(csvpath, compression=None)


def update_tdx_cw_all():
    """
    下载并更新所有通达信专业财务文件
    """
    # 下载通达信财务文件校检信息 http://down.tdx.com.cn:8001/tdxfin/gpcw.txt
    tdx_gpcw_df = pd.DataFrame(
        [
            l.strip().split(",")
            for l in func.download_url(ucfg.tdx["gpcw_url"]).text.strip().split("\r\n")
        ],
        columns=["filename", "md5", "filesize"],
    )
    md5_set = set(tdx_gpcw_df["md5"])

    # 列出本地已有的专业财务文件。
    pattern = "gpcw????????.zip"
    local_zipfile_list = [file.name for file in TDX_CW_PATH.glob(pattern)]

    for row in tdx_gpcw_df.itertuples(index=False):
        filename = row.filename
        if filename not in local_zipfile_list:  # 检查本地文件是否在服务器端存在
            print(f"{filename} 本机没有 开始下载")
            update_tdx_cw(filename)
            print(f"{filename} 完成更新")
        else:  # 计算已有zip文件md5
            with open(TDX_CW_PATH / filename, "rb") as f:
                md5_hash = hashlib.md5()
                while chunk := f.read(4096):
                    md5_hash.update(chunk)
                file_md5 = md5_hash.hexdigest()
            if file_md5 not in md5_set:  # md5与服务端不一致，需要更新
                os.remove(TDX_CW_PATH / filename)
                print(f"{filename} 需要更新 开始下载")
                update_tdx_cw(filename)
                print(f"{filename} 完成更新")
    print("全部专业财务文件检查更新完成")


def parse_gbbq():
    """
    解密通达信股本变迁文件
    """
    print("解密通达信gbbq股本变迁文件")
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
    gbbq_path = TDX_PATH / "T0002/hq_cache/gbbq"
    df_gbbq = pytdx.reader.gbbq_reader.GbbqReader().get_df(gbbq_path)
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
    df_gbbq[["类别", "code"]] = df_gbbq[["类别", "code"]].astype("object")
    df_gbbq["类别"] = df_gbbq["类别"].astype(str).map(category)
    df_gbbq.to_csv(CSV_GBBQ_PATH / "gbbq.csv", encoding="utf-8", index=False)
    print("股本变迁解密完成")


if __name__ == "__main__":
    update_tdx_cw_all()
    parse_gbbq()
