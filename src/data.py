import os
import re
import pickle as pkl

import config as cfg
import utils
import numpy as np
import pandas as pd


class _DataFrameItem(object):
	def __init__(self, data, path, is_optimized=False):
		self.data = data
		self.path = path
		self.is_optimized = is_optimized

	def __getattr__(self, item):
		return self.data.__getattribute__(item)


class DataFramesCollection(object):
	opt_file_prefix = 'opt'

	def __init__(self, exclude=None):
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

		self.collection = {}
		print('Getting the data')
		for name in self.names:
			print('#' * 8 + 'Getting {}'.format(name))
			if os.path.exists(os.path.join(cfg.tmp_path,
			                               '{0}_{1}.pickle'.format(self.opt_file_prefix, name))):
				path = os.path.join(cfg.tmp_path,
				                    '{0}_{1}.pickle'.format(self.opt_file_prefix, name))
				print('#' * 8 + 'Load optimized data from {} file'.format(path))
				with open(path, 'rb+') as f:
					data = pkl.load(f)
				self.collection[name] = _DataFrameItem(data, path, True)
			elif os.path.exists(os.path.join(cfg.input_path,
			                                 '{}.csv'.format(name))):
				path = os.path.join(cfg.input_path,
				                    '{}.csv'.format(name))
				print('#' * 8 + 'Read data from {} file'.format(path))
				data = pd.read_csv(path)
				self.collection[name] = _DataFrameItem(data, path)
			else:
				print('!' * 8 + 'Error, file for {} not found!'.format(name))
		print('Data is ready!')

	def optimize(self, verbose=False):
		print('Starting optimization DataFrames')
		cnt = 0
		for name, item in self.collection.items():
			if not item.is_optimized:
				print('#' * 8 + 'Optimizing {}'.format(name))
				data = utils.reduce_mem_usage(item.data, verbose)
				path = os.path.join(cfg.tmp_path,
				                    '{0}_{1}.pickle'.format(self.opt_file_prefix, name))
				with open(path, 'wb+') as f:
					pkl.dump(data, f)
				self.collection[name] = _DataFrameItem(data, path, True)
				del item
				cnt += 1
		if cnt == 0:
			print('All DataFrames are already optimized!')
		else:
			print('{} DataFrame(s) optimized'.format(cnt))

	def __getattr__(self, item):
		return self.collection.get(item, None)


if __name__ == '__main__':
	collection = DataFramesCollection(exclude="\w*_transaction.csv")
	print(collection.test_identity.shape)
	collection.optimize(verbose=True)

	for name in collection.names:
		item = collection.__getattr__(name)
		if item.is_optimized:
			print(item.memory_usage().sum() / 1024 ** 2)

else:
	collection = DataFramesCollection()
