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


if __name__ == "__main__":

    douban_group = "beijingzufang"

    print("=========================================================")
    print("开始执行，时间：", datetime.strftime(local_time(), "%Y-%m-%d %H:%M:%S") + "\n")

    ## 连接数据库、创建数据表
    rent_info = sqlite3.connect("rent_info.db")
    rent_info.execute(
        f"""CREATE TABLE IF NOT EXISTS {douban_group}
                (title TEXT NOT NULL,
                update_time TEXT NOT NULL,
                link TEXT NOT NULL);"""
    )

    print("数据库连接成功，数据表创建成功\n")

    ## 读取上次执行添加的最新一条数据
    cursor = rent_info.cursor()
    cursor.execute(f"SELECT * FROM {douban_group} ORDER BY update_time DESC")

    try:
        last_update = cursor.fetchone()[1]
    except:  # 当数据表是初次建立时，从4小时前开始爬取
        last_update = datetime.strftime(
            local_time() - timedelta(hours=4), "%m-%d %H:%M"
        )

    ## 一直爬取到上次执行时的最新更新时间
    max_page = 20  # 每次爬取的最大页数（6h执行一次，20页足够包含6h内的更新）
    exit_flag = False
    latest_update = -1
    num = 0

    print("开始爬取...")

    for page in range(1, max_page + 1):  # 从最新页开始爬取

        time.sleep(5)  # 爬取不要太快，防止被封

        page_info = crawl_page(douban_group, page)  # 爬取一页

        if page == 1:
            latest_update = page_info[0][1]

        for item in page_info:  # 遍历当前页中的每一条信息

            if last_update > item[1]:  # 已经爬取到上次执行时的最新更新时间，退出
                exit_flag = True
                break
            else:
                num += 1
                # 提供link查询当前信息是否已经存在，存在则更新，不存在则插入
                cursor.execute(f"SELECT * FROM {douban_group} WHERE link=?", (item[2],))
                row = cursor.fetchone()
                if row:  # 存在则更新
                    cursor.execute(
                        f"UPDATE {douban_group} SET title=?, update_time=? WHERE link=?",
                        (item[0], item[1], item[2]),
                    )
                else:  # 不存在则插入
                    cursor.execute(
                        f"INSERT INTO {douban_group} VALUES (?, ?, ?)",
                        (item[0], item[1], item[2]),
                    )
        if exit_flag:
            break

    rent_info.commit()
    cursor.close()
    rent_info.close()

    print("\n本次爬取结束，共爬取", page, "页", num, "条信息\n")
    print(f"从{last_update}更新至{latest_update}\n")

    db_name = "rent_info.db"  # 数据库名
    table_name = "beijingzufang"  # 豆瓣小组
    xls_write(db_name, table_name)

    print("数据筛选并导出成功\n")
    print("执行完毕，时间：", datetime.strftime(local_time(), "%Y-%m-%d %H:%M:%S"))
    print("=========================================================\n")
