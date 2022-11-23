import requests
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image
import pandas as pd
import re
import mysql.connector


class debtDetail:
    def __init__(self) -> None:
        pass

    def insert_db(self, db_name, data, table_name):
        mydb = mysql.connector.connect(host="localhost", user="root", password="1234")
        mysql_cur = mydb.cursor()
        mysql_cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        mydb = mysql.connector.connect(host="localhost", user="root", password="1234", database=db_name)
        cur = mydb.cursor()
        cols = "`,`".join([str(i) for i in data.columns.tolist()])
        ff = ', '.join(["`"+str(i) + "`" + ' VARCHAR(255)' for i in data.columns.tolist()])
        cur.execute("CREATE TABLE IF NOT EXISTS "+table_name+" ("+ff+")")
        for i, row in data.iterrows():
            sql = "INSERT INTO `"+table_name+"` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cur.execute(sql, tuple(row))
            mydb.commit()

    def main(self, schemeName, partyName):
        s = requests.Session()
        response = s.get('https://drt.gov.in/front/page1_advocate.php')
        soup = BeautifulSoup(response.content, 'lxml')
        self.allSchemesDict = {i.text.strip(): i['value'] for i in soup.find('select', {'id':'schemaname'}).find_all('option')[1:]}
        if schemeName in list(self.allSchemesDict.keys()):
            self.get_table(s, self.allSchemesDict[schemeName], partyName)
        else:
            print(f'{schemeName} name not matching with any Scheme.')
            print('List of available states are : ')
            print(*list(self.allSchemesDict.keys()), sep='\n')

    def get_table(self, session, schemeNumber, partyName):
        extractedInformation = None
        while not str(extractedInformation).strip().isnumeric():
            response = session.get('https://drt.gov.in/front/captcha.php')
            with open('captcha.png', 'wb+') as f:
                f.write(response.content)
            pytesseract.pytesseract.tesseract_cmd = r'OCR\tesseract.exe'
            extractedInformation = pytesseract.image_to_string(Image.open('captcha.png'))

        data = {
            'schemaname': schemeNumber,
            'name': partyName,
            'answer': str(extractedInformation).strip(),
            'submit11': 'Search',
        }

        response = session.post('https://drt.gov.in/front/page1_advocate.php', data=data)
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find_all('table')[1]
        allMoreDetails = ['https://drt.gov.in/drtlive/Misdetailreport.php?no=' + re.search("\('(?P<id>.*)'\)", i.find('a')['href'])['id'] for i in soup.find_all('td') if i.text.strip() == 'MORE DETAIL']
        df = pd.read_html(table.prettify())[0]
        df = df.reset_index()
        for index in range(len(df)):
            response = requests.get(allMoreDetails[index])
            soup = BeautifulSoup(response.content, 'lxml')
            table_rows = [table.find_all("tr") for table in soup.find_all("table")][0]
            for row in table_rows:
                row_data = [td.get_text().strip() for td in row.find_all("td")]
                if "Case Status." in row_data:
                    df.loc[df['index'] == index, "Case Status."] = row_data[1]
            df.loc[df['index'] == index, 'View More'] = allMoreDetails[index]

        df.set_index('index', inplace=True)
        df = df.fillna(" ")
        self.insert_db(db_name="debtDetail", data=df, table_name="debtDetail")
        print('Success')


if __name__ == '__main__':
    obj = debtDetail()
    obj.main('DEBT RECOVERY APPELLATE TRIBUNAL - CHENNAI', 'sha')
