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
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

import func
import user_config as ucfg

# 常量定义
TDX_PATH = Path(ucfg.tdx["tdx_path"])
CW_PATH = TDX_PATH / "vipdoc/cw"
CSV_CW_PATH = Path(ucfg.tdx["csv_cw"])
CSV_GBBQ_PATH = Path(ucfg.tdx["csv_gbbq"])

# 创建必要的目录
CSV_CW_PATH.mkdir(parents=True, exist_ok=True)
CSV_GBBQ_PATH.mkdir(parents=True, exist_ok=True)

start_time = time.time()
print(f"程序启动时间: {time.strftime('%H:%M:%S', time.localtime())}")

# 股本变迁类别映射
GBBQ_CATEGORY = {
    1: "除权除息",
    2: "送配股上市",
    3: "非流通股上市",
    4: "未知股本变动",
    5: "股本变化",
    6: "增发新股",
    7: "股份回购",
    8: "增发新股上市",
    9: "转配股上市",
    10: "可转债上市",
    11: "扩缩股",
    12: "非流通股缩股",
    13: "送认购权证",
    14: "送认沽权证",
}


def process_file(file_name, tdx_txt_df, progress_bar=None):
    """处理单个财务文件：下载、解压、转换为pkl格式"""
    zip_path = CW_PATH / file_name
    dat_path = zip_path.with_suffix(".dat")
    pkl_path = CSV_CW_PATH / file_name.replace(".zip", ".pkl")

    # 下载文件
    zip_url = f"http://down.tdx.com.cn:8001/tdxfin/{file_name}"
    success = func.download_file(zip_url, zip_path)

    if not success:
        if progress_bar:
            progress_bar.update(1)
        return False

    # 解压文件
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(CW_PATH)
    except (zipfile.BadZipFile, OSError) as e:
        print(f"\n解压失败 {file_name}: {e}")
        try:
            zip_path.unlink()
            dat_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False

    # 转换并保存为pkl
    try:
        df = func.historyfinancialreader(str(dat_path))
        df.to_pickle(pkl_path, compression=None)
        return True
    except Exception as e:
        print(f"\n处理财务文件失败 {file_name}: {e}")
        return False


def check_and_update_financial_files(tdx_txt_df):
    """检查和更新财务文件"""
    # 获取现有文件
    local_zips = {f.name for f in CW_PATH.glob("*.zip")}
    existing_pkls = {f.name for f in CSV_CW_PATH.glob("*.pkl")}

    # 需要处理的文件集合
    files_to_process = set()

    # 检查缺失的zip文件
    for server_file in tdx_txt_df["filename"]:
        if server_file not in local_zips:
            files_to_process.add(server_file)

    # 检查需要更新的zip文件
    for local_file in local_zips:
        zip_path = CW_PATH / local_file
        if not zip_path.is_file():
            continue

        try:
            with open(zip_path, "rb") as f:
                local_md5 = hashlib.md5(f.read()).hexdigest()

            # 获取服务器端MD5
            server_info = tdx_txt_df[tdx_txt_df["filename"] == local_file]
            if not server_info.empty:
                server_md5 = server_info.iloc[0]["md5"]
                if local_md5 != server_md5:
                    files_to_process.add(local_file)
        except Exception as e:
            print(f"\n检查文件失败 {local_file}: {e}")

    # 检查缺少的pkl文件
    for dat_file in CW_PATH.glob("*.dat"):
        pkl_file = dat_file.stem + ".pkl"
        if pkl_file not in existing_pkls:
            files_to_process.add(pkl_file.replace(".pkl", ".zip"))

    if not files_to_process:
        print("所有财务文件已是最新状态")
        return

    print(f"需要处理的文件数量: {len(files_to_process)}")

    # 使用线程池处理文件
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(process_file, file_name, tdx_txt_df): file_name
            for file_name in files_to_process
        }

        # 创建进度条
        with tqdm(total=len(futures), desc="更新财务文件") as pbar:
            for future in as_completed(futures):
                file_name = futures[future]
                try:
                    success = future.result()
                    status = "成功" if success else "失败"
                    pbar.set_postfix_str(f"{file_name} {status}")
                except Exception as e:
                    print(f"\n处理文件时发生错误 {file_name}: {e}")
                finally:
                    pbar.update(1)


def process_gbbq():
    """处理股本变迁文件"""
    print("处理股本变迁文件...")
    gbbq_path = TDX_PATH / "T0002/hq_cache/gbbq"

    if not gbbq_path.exists():
        print(f"错误: 股本变迁文件不存在 {gbbq_path}")
        return

    try:
        df_gbbq = pytdx.reader.gbbq_reader.GbbqReader().get_df(str(gbbq_path))
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

        # 映射类别名称
        df_gbbq["类别"] = df_gbbq["类别"].map(GBBQ_CATEGORY).astype("string")
        df_gbbq["code"] = df_gbbq["code"].astype("string")

        # 保存结果
        output_path = CSV_GBBQ_PATH / "gbbq.csv"
        df_gbbq.to_csv(output_path, encoding="gbk", index=False)
        print(f"股本变迁文件已保存至: {output_path}")
    except Exception as e:
        print(f"处理股本变迁文件失败: {e}")


# 主程序
def main():
    # 下载服务器文件列表
    tdx_txt_url = "http://down.tdx.com.cn:8001/tdxfin/gpcw.txt"
    print("获取服务器文件列表...")
    try:
        response = requests.get(tdx_txt_url, timeout=10)
        response.raise_for_status()
        file_list = [line.strip().split(",") for line in response.text.splitlines()]
        tdx_txt_df = pd.DataFrame(file_list, columns=["filename", "md5", "filesize"])
        print(f"获取到 {len(tdx_txt_df)} 个文件信息")
    except Exception as e:
        print(f"获取服务器文件列表失败: {e}")
        return

    # 检查并更新财务文件
    check_and_update_financial_files(tdx_txt_df)

    # 处理股本变迁文件
    process_gbbq()

    # 计算总用时
    elapsed = time.time() - start_time
    print(f"程序完成! 总用时: {elapsed:.2f}秒")


if __name__ == "__main__":
    main()
