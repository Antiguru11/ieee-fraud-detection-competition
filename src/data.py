import os
import re
import pickle as pkl

import config as cfg
import utils
import numpy as np
import pandas as pd


BOOTSTRAP = False
EXCLUDE = None
OPT_FILE_PREFIX = 'opt'


class _DataFrameItem(object):
	def __init__(self, data, path, is_optimized=False):
		self.data = data
		self.path = path
		self.is_optimized = is_optimized


class DataFramesCollection(object):
	def __init__(self, bootstrap=BOOTSTRAP, exclude=EXCLUDE):
		if cfg.__dict__['input_path'] is None or len(cfg.input_path) == 0:
			print('Error, "input_path" variable not found or empty. Please, check "config.py"')

		self.names = []
		for fn in list(os.listdir(cfg.input_path)):
			if fn.endswith('.csv'):
				if type(exclude) is list and fn not in exclude:
					self.names.append(fn[:-4])
				elif type(exclude) is str and re.match(exclude, fn) is None:
					self.names.append(fn[:-4])
				elif exclude is None:
					self.names.append(fn[:-4])
				else:
					print('Error, exclude variable not recognized!')

		self.bootstrap = bootstrap
		self.data_frames = {}
		print('Getting the data')
		for name in self.names:
			print('#' * 8 + 'Getting {}'.format(name))
			data = None
			if os.path.exists(os.path.join(cfg.tmp_path,
			                               '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, name))):
				path = os.path.join(cfg.tmp_path,
				                    '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, name))
				print('#' * 8 + 'Load optimized data from {} file'.format(path))
				if self.bootstrap:
					data = pd.read_pickle(path)
				self.data_frames[name] = _DataFrameItem(data, path, True)
			elif os.path.exists(os.path.join(cfg.input_path,
			                                 '{}.csv'.format(name))):
				path = os.path.join(cfg.input_path,
				                    '{}.csv'.format(name))
				print('#' * 8 + 'Read data from {} file'.format(path))
				if self.bootstrap:
					data = pd.read_csv(path)
				self.data_frames[name] = _DataFrameItem(data, path)
			else:
				print('!' * 8 + 'Error, file for {} not found!'.format(name))

		print('Data is ready!')

	def optimize(self, verbose=False):
		print('Starting optimization DataFrames')
		cnt = 0
		for name, item in self.data_frames.items():
			if not item.is_optimized:
				print('#' * 8 + 'Optimizing {}'.format(name))
				data = item.data
				if not self.bootstrap:
					data = pd.read_csv(item.path)
				opt_data = utils.reduce_mem_usage(data, verbose)
				path = os.path.join(cfg.tmp_path,
				                    '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, name))
				opt_data.to_pickle(path)
				if not self.bootstrap:
					opt_data = None
				self.data_frames[name] = _DataFrameItem(opt_data, path, True)
				cnt += 1
		if cnt == 0:
			print('All DataFrames are already optimized!')
		else:
			print('{} DataFrame(s) optimized'.format(cnt))

	def __getattr__(self, item):
		df_item = self.data_frames.get(item, None)
		if df_item.data is None:
			if df_item.is_optimized:
				data = pd.read_pickle(df_item.path)
			else:
				data = pd.read_csv(self.path)
			return data
		return df_item.data


if __name__ == '__main__':
	collection = DataFramesCollection()
	collection.optimize(verbose=True)

	print(collection.test_identity.head())
	print(collection.train_identity.head())
else:
	collection = DataFramesCollection()
