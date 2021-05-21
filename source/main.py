"""
This file contains database structures, and manipulation API
yabureta urenokauichaba
"""
from flask import Flask
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
from flask_sqlalchemy import SQLAlchemy
from sklearn.utils import resample
from sqlalchemy.orm import relationship
import pandas as pd
from model import Regression_Model
import requests
import datetime

USERNAME = 'root'
PASSWORD = 'qqqq1234'
SERVER = 'localhost'
DB_NAME = 'future_sale'

app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{SERVER}/{DB_NAME}"#.format(USERNAME, PASSWORD, SERVER, DB_NAME)
db = SQLAlchemy(app)

class Item_Categories(db.Model):
	__tablename__ = 'item_categories'
	item_category_id = db.Column(db.Integer, primary_key=True)
	item_category_name = db.Column(db.String(50), nullable=True)
	items = relationship('Items', backref='item_categories', lazy=True)

class Item_Categories_API(Resource):
	item_category_put_args = reqparse.RequestParser()
	item_category_put_args.add_argument('item_category_id', type=int, help='ID of item category', required=False)
	item_category_put_args.add_argument('item_category_name', type=str, help='Name of item category', required=False)
	item_category_put_args.add_argument('csv', type=str, help='Path to upload the csv to Server', required=False)

	item_category_get_args = reqparse.RequestParser()
	item_category_get_args.add_argument('item_category_id', type=int, help='ID of item category', required=False)

	item_category_output = {'item_category_id': fields.Integer, 'item_category_name': fields.String}

	@marshal_with(item_category_output)
	def get(self):
		args = self.item_category_get_args.parse_args()
		id = args['item_category_id']
		if id:
			result = Item_Categories.query.get(id)
		else:
			result = Item_Categories.query.all()
		return result

	def put(self):
		args = self.item_category_put_args.parse_args()
		# try:
		if args['csv']:
			df = pd.read_csv(args['csv'])
			for index, row in df.iterrows():
				isTaken = Item_Categories.query.get(row['item_category_id'])
				if isTaken:
					continue
				buffer = Item_Categories(item_category_id = row['item_category_id'], item_category_name = row['item_category_name'])
				db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		elif args['item_category_id'] and args['item_category_name']:
			isTaken = Item_Categories.query.get(args['item_category_id'])
			if isTaken:
				return {'response': 409}
			buffer = Item_Categories(item_category_id = args['item_category_id'], item_category_name = args['item_category_name'])
			db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		else:
			return {'response': 400}
		# except:
			# return {'response': 500}

	def patch(self):
		args = self.item_category_put_args.parse_args()
		try:
			item_category_id = args['item_category_id']
			result = Item_Categories.query.get(item_category_id).first()
			if not result:
				abort(404, message='Item category doesn\'t exist')
			for key in list(args.keys()):
				if args[key] and key != 'csv':
					result.key = args[key]
			db.session.commit()
			return {'response': 200}
		except:
			return {'response': 500}

	def delete(self):
		args = self.item_category_get_args.parse_args()
		try:
			id = args['item_category_id']
			if id:
				result = Item_Categories.query.get(id)
				db.session.delete(result)
				db.session.commit()
				return {'response': 200}
			return {'response': 400}
		except:
			return {'response': 500}

api.add_resource(Item_Categories_API, '/item_categories')

class Items(db.Model):
	__tablename__ = 'items'
	item_id = db.Column(db.Integer, primary_key=True)
	item_name = db.Column(db.String(200), nullable=True)
	item_category_id = db.Column(db.Integer, db.ForeignKey(Item_Categories.item_category_id))
	# item_categories = relationship('Item_Categories')
	sales = relationship('Sales', backref='items', lazy=True)

class Item_API(Resource):
	item_put_args = reqparse.RequestParser()
	item_put_args.add_argument('item_id', type=int, help='ID of item', required=False)
	item_put_args.add_argument('item_name', type=str, help='Name of item', required=False)
	item_put_args.add_argument('item_category_id', type=int, help='ID of item category', required=False)
	item_put_args.add_argument('csv', type=str, help='Path to upload the csv to Server', required=False)

	item_get_args = reqparse.RequestParser()
	item_get_args.add_argument('item_id', type=int, help='ID of item', required=False)

	item_output = {'item_id': fields.Integer, 'item_name': fields.String, 'item_category_id': fields.Integer}

	@marshal_with(item_output)
	def get(self):
		args = self.item_category_get_args.parse_args()
		id = args['item_id']
		if id:
			result = Items.query.get(id)
		else:
			result = Items.query.all()
		return result

	def put(self):
		args = self.item_put_args.parse_args()
		# try:
		if args['csv']:
			df = pd.read_csv(args['csv'])
			for index, row in df.iterrows():
				isTaken = Items.query.get(row['item_id'])
				isForeignKeyExist = Item_Categories.query.get(row['item_category_id'])
				if isTaken or not isForeignKeyExist:
					continue
				buffer = Items(item_id = row['item_id'], item_name = row['item_name'], item_category_id = row['item_category_id'])
				db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		elif args['item_id'] and args['item_name'] and args['item_category_id']:
			isTaken = Items.query.get(args['item_id'])
			isForeignKeyExist = Item_Categories.query.get(args['item_category_id'])
			if isTaken or not isForeignKeyExist:
				return {'response': 409}
			buffer = Items(item_id = args['item_id'], item_name = args['item_name'], item_category_id = args['item_category_id'])
			db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		else:
			return {'response': 400}
		# except:
		# 	return {'response': 500}

	def patch(self):
		args = self.item_put_args.parse_args()
		try:
			item_id = args['item_id']
			result = Items.query.get(item_id).first()
			if not result:
				abort(404, message='Item doesn\'t exist')
			for key in list(args.keys()):
				if args[key] and key != 'csv':
					result.key = args[key]
			db.session.commit()
			return {'response': 200}
		except:
			return {'response': 500}

	def delete(self):
		args = self.item_get_args.parse_args()
		try:
			id = args['item_id']
			if id:
				result = Items.query.get(id)
				db.session.delete(result)
				db.session.commit()
				return {'response': 200}
			return {'response': 400}
		except:
			return {'response': 500} 

api.add_resource(Item_API, '/items')

class Shops(db.Model):
	__tablename__ = 'shops'
	shop_id = db.Column(db.Integer, primary_key=True)
	shop_name = db.Column(db.String(200), nullable=True)
	sales = relationship('Sales', backref='shops', lazy=True)

class Shops_API(Resource):
	shop_put_args = reqparse.RequestParser()
	shop_put_args.add_argument('shop_id', type=int, help='ID of shop', required=False)
	shop_put_args.add_argument('shop_name', type=str, help='Name of shop', required=False)
	shop_put_args.add_argument('csv', type=str, help='Path to upload the csv to Server', required=False)

	shop_get_args = reqparse.RequestParser()
	shop_get_args.add_argument('shop_id', type=int, help='ID of shop', required=False)

	shop_output = {'shop_id': fields.Integer, 'shop_name': fields.String}

	@marshal_with(shop_output)
	def get(self):
		args = self.shop_get_args.parse_args()
		id = args['shop_id']
		if id:
			result = Shops.query.get(id)
		else:
			result = Shops.query.all()
		return result

	def put(self):
		args = self.shop_put_args.parse_args()
		# try:
		if args['csv']:
			df = pd.read_csv(args['csv'])
			for index, row in df.iterrows():
				isTaken = Shops.query.get(row['shop_id'])
				if isTaken:
					continue
				buffer = Shops(shop_id = row['shop_id'], shop_name = row['shop_name'])
				db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		elif args['shop_id'] and args['shop_name']:
			isTaken = Shops.query.get(args['shop_id'])
			if isTaken:
				return {'response': 409}
			buffer = Shops(shop_id = args['shop_id'], shop_name = args['shop_name'])
			db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		else:
			return {'response': 400}
		# except:
			# return {'response': 500}

	def patch(self):
		args = self.shop_put_args.parse_args()
		try:
			shop_id = args['shop_id']
			result = Shops.query.get(shop_id).first()
			if not result:
				abort(404, message='Shop doesn\'t exist')
			for key in list(args.keys()):
				if args[key] and key != 'csv':
					result.key = args[key]
			db.session.commit()
			return {'response': 200}
		except:
			return {'response': 500}

	def delete(self):
		args = self.shop_get_args.parse_args()
		try:
			id = args['shop_id']
			if id:
				result = Shops.query.get(id)
				db.session.delete(result)
				db.session.commit()
				return {'response': 200}
			return {'response': 400}
		except:
			return {'response': 500} 

api.add_resource(Shops_API, '/shops')

class Sales(db.Model):
	__tablename__ = 'sales'
	date = db.Column(db.DateTime, nullable=False)
	date_block_num = db.Column(db.Integer, nullable=False)
	shop_id = db.Column(db.Integer, db.ForeignKey('shops.shop_id'))
	item_id = db.Column(db.Integer, db.ForeignKey('items.item_id'))
	item_price = db.Column(db.Integer, nullable=False)
	item_cnt_day = db.Column(db.Integer, nullable=False)
	# items = relationship('Items')
	# shops = relationship('Shops')
	__table_args__ = (db.PrimaryKeyConstraint(date, shop_id, item_id), )

class Sales_API(Resource):
	sale_put_args = reqparse.RequestParser()
	sale_put_args.add_argument('date', type=str, help='Name of shop', required=False)
	sale_put_args.add_argument('date_block_num', type=str, help='Name of shop', required=False)
	sale_put_args.add_argument('shop_id', type=int, help='ID of shop', required=False)
	sale_put_args.add_argument('item_id', type=int, help='ID of item', required=False)
	sale_put_args.add_argument('item_price', type=int, help='Price of item', required=False)
	sale_put_args.add_argument('item_cnt_day', type=int, help='Number of item sold that day', required=False)
	sale_put_args.add_argument('csv', type=str, help='Path to upload the csv to Server', required=False)

	sale_get_args = reqparse.RequestParser()
	sale_get_args.add_argument('date', type=str, help='Name of shop', required=False)
	sale_get_args.add_argument('shop_id', type=int, help='ID of shop', required=False)
	sale_get_args.add_argument('item_id', type=int, help='ID of item', required=False)

	sale_output = {'date': fields.DateTime, 'date_block_num': fields.Integer, 'shop_id': fields.Integer, 'item_id': fields.Integer, 'item_price': fields.Integer, 'item_cnt_day': fields.Integer}

	@marshal_with(sale_output)
	def get(self):
		args = self.sale_get_args.parse_args()
		date = args['date']
		shop_id = args['shop_id']
		item_id = args['item_id']
		if date and shop_id and item_id:
			result = Sales.query.filter_by(date=date, shop_id=shop_id, item_id=item_id)
		else:
			result = Sales.query.all()
		return result

	def put(self):
		args = self.sale_put_args.parse_args()
		# try:
		if args['csv']:
			df = pd.read_csv(args['csv'])
			for index, row in df.iterrows():
				isTaken = Sales.query.filter_by(date=row['date'], shop_id=row['shop_id'], item_id=row['item_id']).first()
				isForeignKeyExist1 = Items.query.get(row['item_id'])
				isForeignKeyExist2 = Shops.query.get(row['shop_id'])
				if isTaken or not isForeignKeyExist1 or not isForeignKeyExist2:
					continue
				date = datetime.datetime.strptime(row['date'], "%b %d %Y %H:%M")
				date = date.strftime('%Y-%m-%d %H:%M:%S')
				buffer = Sales(date = date, date_block_num = row['date_block_num'], shop_id = row['shop_id'], item_id = row['item_id'], item_price = row['item_price'], item_cnt_day = row['item_cnt_day'])
				db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		elif args['item_id'] and args['item_name'] and args['item_category_id']:
			isTaken = Sales.query.filter_by(date=args['date'], shop_id=args['shop_id'], item_id=args['item_id']).first()
			isForeignKeyExist1 = Items.query.get(args['item_id'])
			isForeignKeyExist2 = Shops.query.get(args['shop_id'])
			if isTaken or not isForeignKeyExist1 or not isForeignKeyExist2:
				return {'response': 409}
			date = datetime.datetime.strptime(args['date'], "%b %d %Y %H:%M")
			date = date.strftime('%Y-%m-%d %H:%M:%S')
			buffer = Sales(date = date, date_block_num = args['date_block_num'], shop_id = args['shop_id'], item_id = args['item_id'], item_price = args['item_price'], item_cnt_day = args['item_cnt_day'])
			db.session.add(buffer)
			db.session.commit()
			return {'response': 200}
		else:
			return {'response': 400}
		# except:
			# return {'response': 500}

	def patch(self):
		args = self.sale_put_args.parse_args()
		try:
			date = args['date']
			shop_id = args['shop_id']
			item_id = args['item_id']
			result = Sales.query.filter_by(date=date, shop_id=shop_id, item_id=item_id)
			if not result:
				abort(404, message='Sale doesn\'t exist')
			for key in list(args.keys()):
				if args[key] and key != 'csv':
					result.key = args[key]
			db.session.commit()
			return {'response': 200}
		except:
			return {'response': 500}

	def delete(self):
		args = self.sale_get_args.parse_args()
		try:
			date = args['date']
			shop_id = args['shop_id']
			item_id = args['item_id']
			if date and shop_id and item_id:
				result = Sales.query.filter_by(date=date, shop_id=shop_id, item_id=item_id)
				db.session.delete(result)
				db.session.commit()
				return {'response': 200}
			return {'response': 400}
		except:
			return {'response': 500} 

api.add_resource(Sales_API, '/sales')

model = None

class Model_API(Resource):
	model_get_args = reqparse.RequestParser()
	model_get_args.add_argument('input', type=dict, help='Dictionary input for the model', required=True)
	# model_get_args.add_argument('model_id', type=dict, help='Specific model will be used', required=True)

	model_put_args = reqparse.RequestParser()

	def get(self):
		args = self.model_get_args.parse_args()
		input_dict = args['input']
		test_df = pd.DataFrame(input_dict)
		result = model.predict(test_df)
		return {'result': result}
		
	def put(self):
		# args = self.model_put_args.parse_args()
		try:
			item_df = pd.DataFrame(requests.get(BASE + 'items', {'item_id': None}))
			sale_df = pd.DataFrame(requests.get(BASE + 'sales', {'date': None, 'shop_id': None, 'item_id': None}))
			model = Regression_Model(item_df, sale_df)
			model.fit()
			return {'result': model.score(), 'response': 200}
		except:
			return {'result': -1, 'response': 500}

api.add_resource(Model_API, '/model')
# db.create_all()

if __name__ == '__main__':
	app.run(debug=True)