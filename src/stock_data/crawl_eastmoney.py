import pandas as pd
import sqlalchemy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

if __name__ == '__main__':
    browser = webdriver.Chrome()
    # forecasts = pd.DataFrame(columns=['code', 'number_of_reports', 'rank_buy', 'rank_increase', 'rank_neutral',
    #                                   'rank_reduction', 'rank_sell_out', 'eps_2018', 'eps_2019', 'eps_2020',
    #                                   'eps_2021'], index=['0'])
    try:
        browser.get('http://data.eastmoney.com/report/profitforecast.jshtml')
        forecasts = []
        is_not_last_page = True
        while is_not_last_page is True:
            rows = browser.find_elements_by_xpath('.//div[@id="profitforecast_table"]/table/tbody/tr')
            for row in rows:
                columns = row.find_elements_by_xpath('./td')
                forecast = {'code': columns[1].find_element_by_xpath('./a').text,
                            'number_of_reports': columns[4].text,
                            'rank_buy': columns[5].text,
                            'rank_increase': columns[6].text,
                            'rank_neutral': columns[7].text,
                            'rank_reduction': columns[8].text,
                            'rank_sell_out': columns[9].text,
                            'eps_2018': columns[10].text,
                            'eps_2019': columns[11].text,
                            'eps_2020': columns[12].text,
                            'eps_2021': columns[13].text}
                forecasts.append(forecast)

            last_page_btn = browser.find_element_by_xpath('.//*[@id="profitforecast_table_pager"]/div[1]/a[last()]')
            if last_page_btn.text == '下一页':
                last_page_btn.click()
                wait = WebDriverWait(browser, 10)
                wait.until(ec.presence_of_all_elements_located((By.ID, 'profitforecast_table')))
            else:
                is_not_last_page = False

        df_forecasts = pd.DataFrame(forecasts)
        df_forecasts.to_sql('profit_forecast', con=sqlalchemy.create_engine('sqlite:///../../data/em2.db'),
                            if_exists='replace', chunksize=1024)
    finally:
        browser.close()
