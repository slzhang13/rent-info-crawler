# -*- coding: utf-8 -*-

import sqlite3
import time
from util import *
from dateutil import tz
from datetime import datetime, timedelta


def local_time(local_tz="Asia/Shanghai"):
    utc = datetime.utcnow()  # naive datetime object
    utc = utc.replace(tzinfo=tz.gettz("UTC"))  # timezone aware datetime object

    # convert time zone
    local = utc.astimezone(tz.gettz(local_tz))

    return local


# 将添加/更新数据项提取为函数
def add_item(cursor, table_name, item):
    num = 0
    # 根据link查询当前信息是否已经存在，存在且有新回复则更新，不存在则插入
    cursor.execute(f"SELECT * FROM {table_name} WHERE link=?", (item[2],))
    row = cursor.fetchone()
    if row:  # 存在则更新
        if row[1] < item[1]:  # 有新回复
            cursor.execute(
                f"UPDATE {table_name} SET title=?, update_time=? WHERE link=?",
                (item[0], item[1], item[2]),
            )
            num = 1  # 本次更新/添加了一条数据
        else:  # 无新回复
            num = 0  # 本次没有更新/添加数据
    else:  # 不存在则插入
        cursor.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)",
            (item[0], item[1], item[2]),
        )
        num = 1  # 本次更新/添加了一条数据
    return num


def crawl_group(group_id, group_name, cookie=None):
    print("=========================================================")
    print(f"开始爬取豆瓣小组：【{group_name}】\n")
    print(f"url = https://www.douban.com/group/{group_id}/\n")
    print("开始执行，时间：", datetime.strftime(local_time(), "%Y-%m-%d %H:%M:%S") + "\n")

    ## 连接数据库、创建数据表
    rent_info = sqlite3.connect("rent_info.db")
    table_name = group_id if group_id[0].isalpha() else "group_" + group_id

    rent_info.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_name}
                (title TEXT NOT NULL,
                update_time TEXT NOT NULL,
                link TEXT NOT NULL);"""
    )

    print("数据库连接成功，数据表创建成功\n")

    ## 读取上次执行添加的最新一条数据
    cursor = rent_info.cursor()
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY update_time DESC")

    try:
        last_update = cursor.fetchone()[1]
    except:  # 当数据表是初次建立时，从4小时前开始爬取
        last_update = datetime.strftime(local_time() - timedelta(days=5), "%m-%d %H:%M")

    ## 一直爬取到上次执行时的最新更新时间
    max_page = 100  # 每次爬取的最大页数
    latest_update = last_update
    item_num = 0

    print("开始爬取...", end=" ")

    for page in range(1, max_page + 1):  # 从最新页开始爬取
        print(" * ", end="")  # 显示爬取进度
        time.sleep(5)  # 爬取不要太快，防止被封

        page_info = crawl_page(group_id, page, cookie)  # 爬取一页

        item_num_cur_page = 0  # 当前页新添加/更新的数据项
        for item in page_info:
            if len(item[1].split("-")[0]) > 2:  # 针对望京社区小组置顶帖的处理
                # https://www.douban.com/group/304191/
                continue
            latest_update = max(latest_update, item[1])
            item_num_cur_page += add_item(cursor, table_name, item)
        item_num += item_num_cur_page

        if item_num_cur_page == 0:
            break

    print("\n")

    rent_info.commit()
    cursor.close()
    rent_info.close()

    print(f"\n小组【{group_name}】爬取结束，共爬取", page - 1, "页", item_num, "条信息\n")
    print(f"从{last_update}更新至{latest_update}\n")
    print("执行完毕，时间：", datetime.strftime(local_time(), "%Y-%m-%d %H:%M:%S"))
    print("=========================================================\n")


if __name__ == "__main__":
    douban_groups = [
        ("beijingzufang", "北京租房1"),
        ("549574", "北京租房2"),
        ("596202", "北京租房3"),
        ("26926", "北京租房豆瓣"),
        ("hdzufang", "北京海淀租房"),
        ("576564", "西二旗上地房东租房"),
        ("468187", "北京租房&西二旗单间合租房"),
        ("30443", "中关村软件园")
        # ("472358", "北京租房-望京小分队"),
        # ("513717", "望京租房1"),
        # ("537715", "望京租房2"),
        # ("304191", "望京社区"),
    ]

    for group_id, group_name in douban_groups:
        crawl_group(group_id, group_name)

    db_name = "rent_info.db"  # 数据库名

    print("=========================================================")
    print("\n开始导出数据...\n")
    xls_write(db_name, douban_groups)
    print("数据导出成功\n")
    print("=========================================================\n")
