import os
import re
import pickle as pkl

import utils
import config as cfg
import numpy as np
import pandas as pd


OPT_FILE_PREFIX = 'opt'


def _add_metadata(data, **metadata):
	for key in metadata.keys():
		data.__setattr__(key, metadata[key])
	return data


class DataFrameItem(object):
	def __init__(self, name):
		super(DataFrameItem, self).__init__()
		self.name = name
		self._data = None
		self._opt_path = None
		self._input_path = None
		self.optimized = False
		self.bootstraped = False
		self.use = True

		self._input_path = os.path.join(cfg.input_path,
		                                '{}.csv'.format(self.name))
		if os.path.exists(os.path.join(cfg.tmp_path,
		                               '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, self.name))):
			self._opt_path = os.path.join(cfg.tmp_path,
			                              '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, self.name))
			self.optimized = True

	def read(self):
		if self.optimized:
			return pd.read_pickle(self.path)
		else:
			return pd.read_csv(self.path, index=cfg.rep_index_col[self.name])

	def optimize(self):
		data = pd.read_csv(self._input_path, index_col=cfg.rep_index_col[self.name])
		data = utils.reduce_mem_usage(data)
		self._opt_path = os.path.join(cfg.tmp_path,
		                              '{0}_{1}.pickle'.format(OPT_FILE_PREFIX, self.name))
		data.to_pickle(self._opt_path)
		if self.bootstraped:
			self._data = data
		else:
			del data
		self.optimized = True

	def bootstrap(self):
		self._data = self.read()
		self.bootstraped = True

	def remove(self):
		os.remove(self._opt_path)
		os.remove(self._input_path)

	def __getattr__(self, name):
		if name == 'data':
			if self.bootstraped:
				return _add_metadata(self._data,
				                     name_=self.name,
				                     path_=self.path)
			else:
				return _add_metadata(self.read(),
				                     name_=self.name,
				                     path_=self.path)
		elif name == 'path':
			if self.optimized:
				return self._opt_path
			else:
				return self._input_path
		else:
			return None


class DataFramesRepository(object):
	def __init__(self, bootstrap=False, exclude=None):
		super(DataFramesRepository, self).__init__()

		self.data_frames = []
		for fn in list(os.listdir(cfg.input_path)):
			if fn.endswith('.csv'):
				if type(exclude) is list and fn not in exclude:
					self.data_frames.append(DataFrameItem(fn[:-4]))
				elif type(exclude) is str and re.match(exclude, fn) is None:
					self.data_frames.append(DataFrameItem(fn[:-4]))
				elif exclude is None:
					self.data_frames.append(DataFrameItem(fn[:-4]))
				else:
					print('Error, exclude variable not recognized!')

		if bootstrap:
			print('Getting the data')
			for item in self.data_frames:
				print('#' * 8 + 'Load {}'.format(item.name))
				item.bootstrap()

		print('Data is ready!')

	def names(self):
		return [i.name for i in self.data_frames if i.use]

	def use(self, names):
		for item in self.data_frames:
			if item.name in names:
				item.use = True
			else:
				item.use = False

	def optimize(self, names=None, force=False):
		if names is None or len(names) == 0:
			names = self.names()
		print('Starting optimization DataFrames')
		cnt = 0
		for name in names:
			if name not in self.names():
				continue
			if (self[name].optimized and force) or \
			   not self[name].optimized:
				print('#' * 8 + 'Optimizing {}'.format(self[name].name))
				self[name].optimize()
				cnt += 1
		if cnt == 0:
			print('All DataFrames are already optimized!')
		else:
			print('{} DataFrame(s) optimized'.format(cnt))

	def bootstrap(self, names=None, force=False):
		if names is None or len(names) == 0:
			names = self.names()
		if type(names) is list and \
		   len(names) != 0:
			for name in names:
				if name not in self.names():
					continue
				if (self[name].bootstraped and force) or \
				   not self[name].bootstraped:
					self[name].bootstrap()
		else:
			print('Error, parameter "name" not recognized')

	def append(self, name, data, optimize=False, bootstrap=False, force=False):
		if self[name] is not None and not force:
			print('DataFrame {} is already exist!'.format(name))
			return

		print('Start appending {} into repository'.format(name))
		path = os.path.join(DataFramesRepository.input_path,
		                    name + '.csv')
		data.to_csv(path)
		item = DataFrameItem(name)
		self.data_frames.append(item)

		if optimize:
			item.optimize()
		if bootstrap:
			item.bootstrap()
		print('{} successfully appended'.format(name))

	def remove(self, name):
		item = self[name]
		if item is None:
			return
		item.remove()
		self.data_frames.remove(item)

	def __getitem__(self, name):
		for item in self.data_frames:
			if item.name == name:
				return item
		return None

	def __iter__(self):
		return iter([i.data for i in self.data_frames])

	def __getattr__(self, name):
		return None if self[name] is None else self[name].data
