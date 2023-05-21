import re
import os
import sqlite3
import requests
import xlsxwriter as xw


# 爬取豆瓣小组某一页的信息
def crawl_page(douban_group, page, cookie=None):
    url = f"https://www.douban.com/group/{douban_group}/discussion?start={25*(page-1)}&type=new"

    if cookie is None:
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


# 筛选13号线, 16号线沿线独卫房源
def filt_item(title):
    if "独卫" not in title:
        return False
    keywords = [
        "北安河",
        "温阳路",
        "稻香湖路",
        "屯佃",
        "永丰",
        "永丰南",
        "西北旺",
        "马连洼",
        "农大",
        "西二旗",
        "清河",
        "上地",
    ]
    for keyword in keywords:
        if keyword in title:
            return True
    return False


def xls_write(db_name, douban_groups):
    sheet_title = ["标题", "最新回复时间", "链接"]

    filename1 = "douban_rent_info.xlsx"
    workbook1 = xw.Workbook(filename1)

    filename2 = "filtered_rent_info.xlsx"
    workbook2 = xw.Workbook(filename2)

    for group_id, group_name in douban_groups:
        ## 读取数据库
        table_name = group_id if group_id[0].isalpha() else "group_" + group_id
        rows = db_read(db_name, table_name)  # 已排序

        worksheet1 = workbook1.add_worksheet(group_name)
        worksheet1.activate()
        worksheet1.set_column("A:A", 75)
        worksheet1.set_column("B:B", 13)
        worksheet1.set_column("C:C", 50)
        worksheet1.write_row("A1", sheet_title)

        worksheet2 = workbook2.add_worksheet(group_name)
        worksheet2.activate()
        worksheet2.set_column("A:A", 75)
        worksheet2.set_column("B:B", 13)
        worksheet2.set_column("C:C", 50)
        worksheet2.write_row("A1", sheet_title)

        cnt = 0
        for i, row in enumerate(rows):
            title = row[0]
            idx = "A" + str(i + 2)
            worksheet1.write_row(idx, row)

            if filt_item(title):
                idx = "A" + str(cnt + 2)
                worksheet2.write_row(idx, row)
                cnt += 1

    workbook1.close()
    workbook2.close()
