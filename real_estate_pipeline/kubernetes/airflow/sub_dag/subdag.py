from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
import requests
import pandas as pd
import mysql.connector
from time import sleep
import re

url = 'https://www.nhatot.com/mua-ban-bat-dong-san-ha-noi?page=1'

def get_product_status(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    status = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'property_status'})

    status_result = status.text if status else None
    return status_result


def get_product_price_m2(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    move = 'triệu/m²'
    move2 = 'tỷ/m²'
    move3 = 'đ/m²'
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'price_m2'})

    m2_result = m2.text if m2 else None

    if move in m2_result:
        m2_result = m2_result.replace(move, '') if move in m2_result else m2_result
        m2_result = m2_result.replace(',', '.')
        m2_result = float(m2_result)

    elif move2 in m2_result:
        m2_result = m2_result.replace(move2, '') if move2 in m2_result else m2_result
        m2_result = m2_result.replace(',', '.')
        m2_result = float(m2_result)
        m2_result = m2_result * 1000

    elif move3 in m2_result:
        m2_result = m2_result.replace(move3, '') if move3 in m2_result else m2_result
        m2_result = m2_result.replace(',', '.')
        m2_result = float(m2_result)
        m2_result = m2_result / 1000

    return m2_result


def get_product_toilet(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'toilets'})

    if not m2:
        return None

    m2_result = m2.text
    m2_result = re.findall(r'\d+', m2_result)

    # Chuyển kết quả từ danh sách thành số nguyên (nếu có)
    m2_result = int(m2_result[0])
    return m2_result


def get_product_room(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'rooms'})
    if not m2:
        return None

    m2_result = m2.text
    m2_result = re.findall(r'\d+', m2_result)

    # Chuyển kết quả từ danh sách thành số nguyên (nếu có)
    m2_result = int(m2_result[0])
    return m2_result


def get_product_legal_doc(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'property_legal_document'})

    m2_result = m2.text if m2 else None
    return m2_result


def get_product_floor(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'floors'})
    if not m2:
        return None

    m2_result = m2.text if m2 else None
    m2_result = int(m2_result)
    return m2_result


def get_product_type(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'house_type'})

    m2_result = m2.text if m2 else None
    return m2_result


def get_product_furnishing(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    m2 = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'furnishing_sell'})

    m2_result = m2.text if m2 else None
    return m2_result


def get_product_area(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    move = 'm²'
    area = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'size'})

    area_result = area.text if area else None

    area_result = area_result.replace(move, '') if move in area_result else area_result
    area_result = area_result.replace(',', '.')
    area_result = float(area_result)
    return area_result


def get_product_location(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    location = page_source.find('span', {'class': 'bwq0cbs flex-1'})

    location_result = location.text if location else None
    return location_result


def get_product_name(page_source):
    move = 'Nhấn để xem thông tin về dự án được sàng lọc uy tín và sát sao nhất thị trường'
    # Tìm thẻ <span> với class và itemprop cụ thể
    name_result = page_source.find('span', {'class': 'AdParam_adParamValue__IfaYa', 'itemprop': 'new_project'})

    # Tìm thẻ <a> bên trong thẻ <span> đó
    if name_result:
        a_tag = name_result.find('a')
        if a_tag:
            a_content = a_tag.text if a_tag else None
            a_content = a_content.replace(move, '') if move in a_content else a_content
        else:
            return None
        return a_content
    return None


def get_product_price(page_source):
    # Tìm thẻ <span> với class và itemprop cụ thể
    price = page_source.find('b', class_='pyhk1dv')

    move = 'tỷ'
    move1 = 'triệu'
    move2 = 'đ'

    price_result = price.text if price else None
    if price:
        price_result = price.text
        if move1 in price_result:
            price_result = price_result.replace(move1, '')
            price_result = price_result.replace(',', '.')
            price_result = float(price_result)
            price_result = price_result / 1000
        elif move2 in price_result:
            price_result = price_result.replace(move2, '')
            price_result = price_result.replace(',', '.')
            price_result = float(price_result)
            price_result = price_result / 1000000

        else:
            price_result = price_result.replace(move, '') if move in price_result else price_result
            price_result = price_result.replace(',', '.')
            price_result = float(price_result)
        return price_result
    else:
        return None

    return None


def quit_then_back(url):
    driver1 = webdriver.Chrome()
    driver1.get(url)
    sleep(1)

    page_source = BeautifulSoup(driver1.page_source, "html.parser")
    button = page_source.find('button', class_='styles_button__SVZnw styles_buttonPty__POBG4')

    if button:
        more_button = driver1.find_element(By.CLASS_NAME, 'styles_button__SVZnw.styles_buttonPty__POBG4')
        driver1.execute_script("arguments[0].click();", more_button)

    new_page_source = BeautifulSoup(driver1.page_source, "html.parser")

    name_result = get_product_name(new_page_source)
    price_result = get_product_price(new_page_source)
    location_result = get_product_location(new_page_source)
    status_result = get_product_status(new_page_source)
    area_result = get_product_area(new_page_source)
    price_m2_result = get_product_price_m2(new_page_source)
    toilet_result = get_product_toilet(new_page_source)
    room_result = get_product_room(new_page_source)
    doc_result = get_product_legal_doc(new_page_source)
    type_result = get_product_type(new_page_source)
    floor_result = get_product_floor(new_page_source)
    furnishing_result = get_product_furnishing(new_page_source)
    return name_result, price_result, location_result, status_result, area_result, price_m2_result, toilet_result, room_result, doc_result, type_result, floor_result, furnishing_result


def get_product_one_page(url):
    driver = webdriver.Chrome()
    driver.get(url)
    page_source = BeautifulSoup(driver.page_source, "html.parser")
    links = page_source.find_all("a", attrs={'itemprop': 'item'})
    #     for link in links:
    #         href = link.get('href')
    #         if href and not href.startswith('https'):
    #             print(href)

    move = '?page='
    url = url.replace(move, '') if move in url else url

    list_names = []
    list_price = []
    list_location = []
    list_status = []
    list_area = []
    list_price_m2 = []
    list_toilet = []
    list_room = []
    list_doc = []
    list_type = []
    list_floor = []
    list_furnishing = []
    # for item in range(len(links)):

    for item in range(3):
        href = links[item].get('href')
        if href and not href.startswith('https'):
            link = href
            true_link = url + link
            name_item, price_item, location_item, status_item, area_item, price_m2_item, toilet_item, room_item, doc_item, type_item, floor_item, furnishing_item = quit_then_back(
                true_link)

            #       name_item = name_result[item].text
            #       area_item = area_result[item].text
            #       price_item = price_result[item].text

            list_names.append(name_item)
            list_price.append(price_item)
            list_location.append(location_item)
            list_status.append(status_item)
            list_area.append(area_item)
            list_price_m2.append(price_m2_item)
            list_toilet.append(toilet_item)
            list_room.append(room_item)
            list_doc.append(doc_item)
            list_type.append(type_item)
            list_floor.append(floor_item)
            list_furnishing.append(furnishing_item)
    #       list_time.append(time_item)
    #        item = item+1

    #     for item in range(len(location_result)):
    #         if 'Huyện' in location_result[item].text:
    #             location_item = location_result[item].text
    #             list_location.append(location_item)
    #         if 'Quận' in location_result[item].text:
    #             location_item = location_result[item].text
    #             list_location.append(location_item)
    #         item = item+1

    df = pd.DataFrame()
    df['du_an'] = list_names
    df['price_VND'] = list_price
    df['location'] = list_location
    df['status'] = list_status
    df['area'] = list_area
    df['price_m2'] = list_price_m2
    df['toilet'] = list_toilet
    df['room'] = list_room
    df['doc'] = list_doc
    df['type'] = list_type
    df['Số tầng'] = list_floor
    df['furnishing'] = list_furnishing

    return df


def get_product_all_pages(**kwargs):
    all_page = pd.DataFrame()

    for page in range(1):  # Điều chỉnh số lượng trang theo nhu cầu
        print(f'Trang số: {page + 1}')
        url_s = 'https://www.nhatot.com/mua-ban-bat-dong-san-ha-noi' + '?page=' + f'{page + 1}'
        one_page = get_product_one_page(url_s)
        all_page = pd.concat([all_page, one_page], ignore_index=True)

    # Push data to XCom using kwargs (Airflow's task instance)
    kwargs['ti'].xcom_push(key='crawled_data', value=all_page.to_dict())

    # Lưu DataFrame all_page vào file CSV
    all_page.to_csv('/home/hoang/crawled_data.csv', index=False, encoding='utf-8')

    return all_page


def save_crawled_data_to_csv(**kwargs):
    # Lấy dữ liệu từ XCom
    crawled_data_dict = kwargs['ti'].xcom_pull(task_ids='get_product_all_pages', key='crawled_data')

    if crawled_data_dict is not None:
        # Chuyển đổi dict thành DataFrame
        all_data = pd.DataFrame.from_dict(crawled_data_dict)

        # Lưu DataFrame vào file CSV
        try:
            all_data.to_csv('/home/hoang/products.csv', index=False)
            print("Dữ liệu đã được lưu vào file crawled_data.csv thành công.")
        except Exception as e:
            print(f"Đã xảy ra lỗi khi lưu dữ liệu: {e}")
    else:
        print("Dữ liệu đã được lưu vào file crawled_data.csv thành công")

def load_csv_to_mysql(**kwargs):
    # Đọc file CSV
    file_path = '/home/hoang/crawled_data.csv'
    df = pd.read_csv(file_path)

    # Kết nối tới MySQL
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='datamaking',
        database='demo'
    )
    cursor = conn.cursor()

    # Tạo bảng trước nếu chưa tồn tại
    create_table_query = """
    CREATE TABLE IF NOT EXISTS demo (
        du_an VARCHAR(255),
        price_VND FLOAT,
        location VARCHAR(255),
        status VARCHAR(100),
        area FLOAT,
        price_m2 FLOAT,
        toilet INT,          
        room INT,            
        doc VARCHAR(100),
        type VARCHAR(100),
        So_tang INT,        
        furnishing VARCHAR(100)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Xóa dữ liệu cũ trong bảng trước khi chèn dữ liệu mới
    delete_query = "DELETE FROM demo"
    cursor.execute(delete_query)
    conn.commit()

    # Chèn từng dòng dữ liệu từ DataFrame vào bảng MySQL
    for index, row in df.iterrows():
        sql = """
        INSERT INTO demo (du_an, price_VND, location, status, area, price_m2, toilet, room, doc, type, So_tang, furnishing) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        val = (
            row['du_an'],
            row['price_VND'],
            row['location'],
            row['status'],
            row['area'],
            row['price_m2'],
            row['toilet'],
            row['room'],
            row['doc'],
            row['type'],
            row['So_tang'],  # Chú ý: Nếu cột trong CSV là 'Số tầng', cần điều chỉnh tên này trong DataFrame
            row['furnishing']
        )
        cursor.execute(sql, val)

    # Lưu thay đổi
    conn.commit()

    print("Dữ liệu đã được nhập thành công!")

    # Đóng kết nối
    cursor.close()
    conn.close()

