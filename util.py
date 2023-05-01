import re
import os
import sqlite3
import requests
import xlsxwriter as xw


# 爬取豆瓣小组某一页的信息
def crawl_page(douban_group, page):
    url = f"https://www.douban.com/group/{douban_group}/discussion?start={25*(page-1)}&type=new"

    cookie = os.environ.get("COOKIE")  # get from repository secrets
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        "Cookie": cookie,
    }

    # raw text
    html_raw = requests.get(url, headers=headers).text

    # raw items
    items_raw = re.findall(r"<tr class=\"\">([\s\S]*?)<\/tr>", html_raw)

    page_info = []
    for i in range(len(items_raw)):
        item = items_raw[i]
        item = re.sub(" +", " ", item.replace("\n", " "))
        link_title = re.findall(r"<a href=\"(.*?)\"\s+title=\"(.*?)\"", item)
        update = re.findall(r"class=\"time\">(.*?)<\/td>", item)
        page_info.append([link_title[0][1], update[0], link_title[0][0]])

    return page_info


# 读取数据表内容，且按照update_time排序
def db_read(db_name, table_name, order_by="update_time", order="DESC"):
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY {order_by} {order}")
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return rows


# 按关键字对标题筛选
def filt_item(title):
    # need further definition
    return True if "求租" not in title else False  # 排除求租信息


def xls_write(db_name, douban_groups):
    filename = "douban_rent_info.xlsx"
    workbook = xw.Workbook(filename)

    for group_id, group_name in douban_groups:
        ## 读取数据库
        rows = db_read(db_name, group_id)  # 已排序

        ## 筛选并写入xlsx
        worksheet = workbook.add_worksheet(group_name)
        worksheet.activate()
        # 按照实际格式调整
        worksheet.set_column("A:A", 75)
        worksheet.set_column("B:B", 13)
        worksheet.set_column("C:C", 50)
        sheet_title = ["标题", "最新回复时间", "链接"]

        worksheet.write_row("A1", sheet_title)

        for i, row in enumerate(rows):
            title = row[0]
            if filt_item(title):
                idx = "A" + str(i + 2)
                worksheet.write_row(idx, row)

    workbook.close()
