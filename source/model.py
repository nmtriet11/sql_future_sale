import pandas as pd
# import numpy as np
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from tqdm import tqdm

class Regression_Model():
	def __init__(self, item_df, sale_df):
		self.item_df = item_df
		self.sale_df = sale_df

		self.X = None
		self.y = None
		# self.X_val = None
		# self.y_val = None
		
		self.model = XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8)

	def load(self, path):
		self.model.load_model(path)

	def save(self, path):
		self.model.save_model(path)

	def _new_item_category(self):
		new_cat_dict = {
			(0, 0): 0,
			(1, 7): 1, 
			(8, 8): 2, 
			(9, 9): 3,
			(10, 17): 4, 
			(18, 24): 5,
			(25, 25): 6,
			(26, 27): 7, 
			(28, 31): 8,
			(32, 36): 9,
			(37, 42): 10, 
			(43, 54): 11,
			(55, 60): 12, 
			(61, 72): 13,
			(73, 78): 14, 
			(79, 80): 15,
			(81, 82): 16, 
			(83, 83): 17,
		}
		for key in list(new_cat_dict.keys()):
			self.X.loc[(self.X['item_category_id'] >= key[0]) & (self.X['item_category_id'] <= key[1]), 'item_category_id'] = new_cat_dict[key]

	def _new_feature_train(self):	
		month_gap = [1, 2, 3, 6, 12]
		for month in tqdm(month_gap):	
			# Add previous item/shop sales
			temp_df = self.X.copy()
			feature_name_1 = 'pre_item_shop_sale_' + str(month)	
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_cnt_month': feature_name_1})
			self.X = self.X.merge(temp_df[['shop_id', 'item_id', 'date_block_num', feature_name_1]], 
												on = ['shop_id', 'item_id', 'date_block_num'], how = 'left')
			self.X[feature_name_1] = self.X[feature_name_1].fillna(0)

			# Add previous item sales
			temp_df = self.X.groupby(['date_block_num', 'item_id']).agg({'item_cnt_month':'mean'})
			temp_df = temp_df.reset_index()
			feature_name_2 = 'pre_item_sale_' + str(month)
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_cnt_month': feature_name_2})
			self.X = self.X.merge(temp_df[['item_id', 'date_block_num', feature_name_2]], on = ['item_id', 'date_block_num'], how = 'left')
			self.X[feature_name_2] = self.X[feature_name_2].fillna(0)

			# Add previous price/shop		
			temp_df = self.X.groupby(['date_block_num', 'item_id', 'shop_id']).agg({'item_price':'mean'})
			temp_df = temp_df.reset_index()
			feature_name_3 = 'pre_price_' + str(month) 
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_price': feature_name_3})
			self.X = self.X.merge(temp_df[['shop_id', 'item_id', 'date_block_num', feature_name_3]], on = ['item_id', 'date_block_num', 'shop_id'], how = 'left')
			self.X[feature_name_3] = self.X[feature_name_3].fillna(0)
	
	def _preprocess_train(self):
		"""
		Function: Preprocess the training data
		"""
		# Negative item price is unacceptable
		train_df = self.sale_df[self.sale_df['item_price'] >= 0]
		
		train_df = train_df.groupby(['date_block_num', 'shop_id', 'item_id']).agg({'item_cnt_day': 'sum', 'item_price': 'mean', 'date': 'first'})
		train_df = train_df.rename(columns={'item_cnt_day': 'item_cnt_month'})
		train_df = train_df.reset_index()
		train_df = train_df.join(self.item_df.set_index('item_id'), on='item_id').drop('item_name', axis=1)
		self.X = train_df

		# Let's change the training set
		self._new_item_category()
		self._new_feature_train()

		self.y = self.X['item_cnt_month']
		self.X = self.X.drop(['item_cnt_month', 'date'], axis='columns')
		

	def _preprocess_test(self, df, now_block_num = 34):
		df = df.drop('ID', axis=1)
		df['date_block_num'] = now_block_num

		temp_df = self.X[['shop_id', 'item_id', 'item_price']].groupby(['shop_id', 'item_id']).agg({'item_price': 'mean'})
		temp_df = temp_df.reset_index()
		df = df.merge(temp_df, on=['shop_id', 'item_id'], how='inner')
		df = df.join(self.item_df.set_index('item_id'), on='item_id').drop('item_name', axis=1)

		new_cat_dict = {
			(0, 0): 0,
			(1, 7): 1, 
			(8, 8): 2, 
			(9, 9): 3,
			(10, 17): 4, 
			(18, 24): 5,
			(25, 25): 6,
			(26, 27): 7, 
			(28, 31): 8,
			(32, 36): 9,
			(37, 42): 10, 
			(43, 54): 11,
			(55, 60): 12, 
			(61, 72): 13,
			(73, 78): 14, 
			(79, 80): 15,
			(81, 82): 16, 
			(83, 83): 17,
		}
		for key in list(new_cat_dict.keys()):
			df.loc[(df['item_category_id'] >= key[0]) & (df['item_category_id'] <= key[1]), 'item_category_id'] = new_cat_dict[key]

		month_gap = [1, 2, 3, 6, 12]
		for month in tqdm(month_gap):	
			# Add previous item/shop sales
			temp_df = self.X.copy()
			feature_name_1 = 'pre_item_shop_sale_' + str(month)	
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_cnt_month': feature_name_1})
			df = df.merge(temp_df[['shop_id', 'item_id', 'date_block_num', feature_name_1]], 
												on = ['shop_id', 'item_id', 'date_block_num'], how = 'left')
			df[feature_name_1] = df[feature_name_1].fillna(0)

			# Add previous item sales
			temp_df = self.X.groupby(['date_block_num', 'item_id']).agg({'item_cnt_month':'mean'})
			temp_df = temp_df.reset_index()
			feature_name_2 = 'pre_item_sale_' + str(month)
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_cnt_month': feature_name_2})
			df = df.merge(temp_df[['item_id', 'date_block_num', feature_name_2]], on = ['item_id', 'date_block_num'], how = 'left')
			df[feature_name_2] =df[feature_name_2].fillna(0)

			# Add previous price/shop		
			temp_df = self.X.groupby(['date_block_num', 'item_id', 'shop_id']).agg({'item_price':'mean'})
			temp_df = temp_df.reset_index()
			feature_name_3 = 'pre_price_' + str(month) 
			temp_df['date_block_num'] += month
			temp_df = temp_df.rename(columns={'item_price': feature_name_3})
			df = df.merge(temp_df[['shop_id', 'item_id', 'date_block_num', feature_name_3]], on = ['item_id', 'date_block_num', 'shop_id'], how = 'left')
			df[feature_name_3] = df[feature_name_3].fillna(0)

		return df

	def fit(self, val_size=.2):	
		self._preprocess_train()
		self.X, self.X_val, self.y, self.y_val = train_test_split(self.X, self.y, test_size=val_size, random_state=11)
		self.model.fit(self.X, self.y)
		print('Model done fitting!')
		
	def predict(self, test_df):
		copy_df = test_df.copy()
		copy_df = self._preprocess_test(copy_df)
		result = self.model.predict(copy_df)
		return result

	def score(self, X_test=None, y_test=None):
		if X_test==None and y_test==None:
			X_test = self.X_val
			y_test = self.y_val
		y_pred = self.predict(X_test)
		predictions = [round(value) for value in y_pred]
		accuracy = accuracy_score(y_test, predictions)
		return accuracy


# item_cat_df = pd.read_csv('../dataset/item_categories.csv')
# item_df = pd.read_csv('../dataset/items.csv')
# sales_train_df = pd.read_csv('../dataset/sales_train.csv')
# sample_submission_df = pd.read_csv('../dataset/sample_submission.csv')
# shops_df = pd.read_csv('../dataset/shops.csv')
# test_df = pd.read_csv('../dataset/test.csv')

# aa = Regression_Model(item_df, sales_train_df)
# aa.fit()
# print(aa.score())