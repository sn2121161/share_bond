# coding:utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pl
from logs import logger


def get_webpage_content(url):
    # 设置Chrome选项
    # 基本设置
    chrome_options = Options()

    chrome_options.add_argument("--headless=new")
    # chrome_options.add_argument("--start-maximized")  # 最大化窗口
    chrome_options.add_argument("--log-level=3")  # 新增日志级别控制
    chrome_options.add_argument("--disable-infobars")  # 禁用浏览器正在被自动化程序控制的提示
    chrome_options.add_argument("--disable-extensions")  # 禁用扩展

    # 写进日志
    # 启用捕获网络请求的功能
    # capabilities = DesiredCapabilities.CHROME
    # capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

    # 设置Chrome驱动路径
    chrome_driver_path = r'/Users/nzw/Documents/chromeDriver/chromedriver-mac-arm64/chromedriver'  # 请将此路径替换为实际的chromedriver路径
    service = Service(chrome_driver_path)


    # 启动Chrome浏览器
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)#

    # 点击登录按钮跳转
    login_xpath = '/html/body/div/div/div[1]/div/div/div[4]/div/div/button[1]'

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.XPATH, login_xpath))
    )

    # 点击进入登录界面
    login_button = driver.find_element(By.XPATH, login_xpath)
    login_button.click()
    logger.info("进入登陆界面")
    # logger.info("")

    username = "17751592979"  # 替换为实际用户名
    password = "hyhcz1996"  # 替换为实际密码
    # 找到输入用户名和密码的地方
    username_xpath = '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[2]/input'
    password_xpath = '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[3]/input'
    remember_xpath = '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[5]/div[1]/input'
    checkbox_xpath = '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[5]/div[2]/input'
    login_xpath2 = '/html/body/div[3]/div/div/div[1]/div[1]/div[3]/form/div[6]'
    # my_study_xpath = '//*[@id="navMenusAll"]/div[3]/a'

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.XPATH, login_xpath2))
    )

    # 输入用户名密码
    username_input = driver.find_element(By.XPATH, username_xpath)
    password_input = driver.find_element(By.XPATH, password_xpath)
    username_input.send_keys(username)
    password_input.send_keys(password)

    # 点击记住我，下次不用输入密码了
    remember = driver.find_element(By.XPATH, remember_xpath)
    remember.click()

    # 勾选已同意两个条款
    checkbox = driver.find_element(By.XPATH, checkbox_xpath)
    checkbox.click()
    # print(checkbox.text)


    # 点击登录按钮
    login_button = driver.find_element(By.XPATH, login_xpath2)
    login_button.click()
    logger.info("登陆成功")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'nav-data'))
    )
    return driver


def refresh_data(driver):
    # 刷新
    refresh_xpath = '/html/body/div/div/div[2]/div[2]/div[2]/div[2]/div[1]/div/div[1]/span/a'
    # /html/body/div/div/div[2]/div[2]/div[2]/div[2]/div[1]/div/div[1]/span/a/span
    # refresh_box_xpath = '/html/body/div/div/div[2]/div[2]/div[2]/div[2]/div[1]/div/div[1]/span/label/span[1]'
    refresh = driver.find_element(By.XPATH, refresh_xpath)
    refresh.click()
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'nav-data'))
    )
    logger.info("刷新成功")
    # 表头
    nav_data = driver.find_element(By.CLASS_NAME, 'nav-data')
    # table_head = nav_data.find_element(By.XPATH, '/html/body/div/div/div[2]/div[2]/div[2]/div/div[2]/div[1]')
    table_head = nav_data.find_element(By.CSS_SELECTOR, 'div.jsl-table-header-wrapper')

    list_container = WebDriverWait(table_head, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr"))
    )

    items = list_container.find_elements(By.CSS_SELECTOR, "th")
    title = list(map(lambda x: x.text.replace("\n", ""), items))
    logger.info("表头采集完成")

    # 表体
    table_body = nav_data.find_element(By.CSS_SELECTOR, 'div.jsl-table-body-wrapper')
    list_body = WebDriverWait(table_body, 10).until(
            # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table > tbody"))
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr"))
        )
    # titles=30，筛选掉body_items中长度小于30的
    bodies = list(
        map(lambda row: [item for item in row if item.strip() != ''],  # 第3步：清理空字符串
            filter(lambda x: len(x) >= 30 and ('退债' not in ' '.join(x)),  # 第2步：过滤
                map(lambda x: x.text.replace('\n', " ").split(' '), list_body)  # 第1步：转换
            )
        )
    )
    logger.info("表体采集完成")
    df = pl.DataFrame(bodies, columns=title)
    return df