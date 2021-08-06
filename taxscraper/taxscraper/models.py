import datetime
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