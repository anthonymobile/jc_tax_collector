import requests
from bs4 import BeautifulSoup
import dateutil.parser
import datetime
import argparse
import logging

from sqlalchemy import create_engine, Column, Integer, Numeric, String, Date, ForeignKey
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base

# config
base_url='http://taxes.cityofjerseycity.com'

# schema setup
Base = declarative_base()

class Property(Base):

    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)

    __tablename__ = 'properties'
    id = Column(Integer, primary_key=True, autoincrement=True)
    AccountNumber = Column(Integer)
    Block = Column(String) #integer?
    Lot = Column(String) #integer?
    Qualifier  = Column(String) #integer?
    OwnersName = Column(String)
    PropertyLocation = Column(String)
    ViewPayLink = Column(String)
    transactions = relationship("Transaction", back_populates="property")

    def __repr__(self):
        return f"{self.AccountNumber}\t{self.Block} {self.Lot} {self.Qualifier} \t{self.PropertyLocation}\t {self.OwnersName}\t {self.ViewPayLink}"


class Transaction(Base):

    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)

    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    Year = Column(Integer)
    Qtr = Column(Integer)
    TrDueDate = Column(Date)
    Description = Column(String)
    Billed = Column(Numeric)
    Paid = Column(Numeric)
    OpenBalance = Column(Numeric)
    Days = Column(Integer)
    InterestDue = Column(Numeric)
    PaidBy = Column(String)

    property_id = Column(Integer, ForeignKey('properties.id'))
    property = relationship("Property", back_populates="transactions")

    def __repr__(self):
        return f"{self.property_id}\t{self.TrDueDate}\t{self.Description}\t{self.Billed}\t{self.Paid}\t{self.OpenBalance}\t{self.Days}\t{self.InterestDue}\t{self.PaidBy}\t"


def init_db(disk_flag):

    if disk_flag is True:
        now=datetime.datetime.now().strftime('%x_%X').replace('/','-')
        db_url = f'sqlite:///jc_taxcollector{now}.sqlite3'
        logging.info(f'writing to disk {db_url}')
        engine = create_engine(db_url)
    elif disk_flag is False:
        engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def dump_to_db(session, records):
    for r in records:
        session.add(r)
    session.commit()
    return


def get_page_range():
    url = f'{base_url}/?page=1'
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    last_page_url = soup.find_all('a', class_='btn-primary')[-1]['href']
    last_page_num = int(last_page_url.split('=')[1])
    return range(1, last_page_num)


def fetch_properties(url):
    property_index = BeautifulSoup(requests.get(url).content,"html.parser")
    return parse_properties(property_index)


def parse_properties(property_index):

    properties = []

    rows = property_index.find_all('tr')[1:]

    for row in rows:
        data =row.find_all('td')
        property_dictionary = {
            'AccountNumber' : data[0].text.strip(),
            'Block' : data[1].text.strip(),
            'Lot' : data[2].text.strip(),
            'Qualifier' : data[3].text.strip(),
            'OwnersName' : data[4].text.strip(),
            'PropertyLocation' : data[5].text.strip(),
            'ViewPayLink' : row.find('a')['href']
        }

        parsed_record = Property(property_dictionary)
        logging.info(parsed_record)

        # todo check for dupes, updates

        properties.append(parsed_record)

    return properties


def fetch_transactions(property):

    transaction_index = BeautifulSoup(
        requests.get(
            base_url + property.ViewPayLink).content,
        "html.parser"
    )
    logging.info(base_url + property.ViewPayLink)
    transactions = parse_transactions(property, transaction_index)
    # for transaction in transactions:
    #     logging.info(transaction)
    return transactions

def filter_transaction_rows(rows):

    # skip first table on these kinds http://taxes.cityofjerseycity.com/ViewPay?accountNumber=2162
    # finds the row that starts with td 'Year' and then chop everything after that

    counter = 0
    for row in rows:
        data = row.find_all('th')
        if len(data) >0:
            if data[0].text.strip() == 'Year':
                return rows[(counter+1):]
        counter += 1


def parse_transactions(property, transaction_index):

    transactions = []
    rows = transaction_index.find_all('tr')
    rows = filter_transaction_rows(rows)

    for row in rows:
        data =row.find_all('td')

        transaction_dictionary = {
            'property_id' : property.id,
            'Year' : data[0].text.strip(),
            'Qtr' : data[1].text.strip(),
            'TrDueDate' : dateutil.parser.parse(
                data[2].text.strip()
            ),
            'Description' : data[3].text.strip(),
            'Billed' : clean_money(data[4].text.strip()),
            'Paid' : clean_money(data[5].text.strip()),
            'OpenBalance' : clean_money(data[6].text.strip()),
            'Days' : data[7].text.strip(),
            'InterestDue' : clean_money(data[8].text.strip()),
            'PaidBy' : data[9].text.strip(),
        }

        parsed_record = Transaction(transaction_dictionary)
        # logging.info(parsed_record)
        transactions.append(parsed_record)

    return transactions


def clean_money(string):
    no_dollar = string.translate({36: None})
    no_comma = no_dollar.translate({44: None})
    if no_comma[0] == '(':
        no_comma = no_comma[1:-1]
    else:
        pass
    return no_comma

##################################################################

if __name__ == '__main__':

    # logging
    import logging
    import sys

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    file_handler = logging.FileHandler('logs.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)

    # parse arguments
    parser = argparse.ArgumentParser(description='JC TaxCollector v0.1')
    parser.add_argument('-d', '--disk', action='store_true', help="Store scraped data on disk (vs memory) in sqlite3 file.")
    args = parser.parse_args()


    with init_db(args.disk) as session:

            # get home page
            page_range = get_page_range()

            for page_num in page_range:

                property_index_url = f'{base_url}/?page={page_num}'
                logging.info(f'fetching properties for page {page_num} \n {property_index_url}')
                properties = fetch_properties(property_index_url)
                dump_to_db(session, properties)

                # get and dump transactions for each property
                for property in properties:

                    logging.info(f'fetching tranasctions for property {property.id}')
                    transactions = fetch_transactions(property)
                    dump_to_db(session, transactions)
