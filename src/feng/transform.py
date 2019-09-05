import numpy as np
import pandas as pd

from data import repository as rep


class TransformerFunction(object):
	def __init__(self, name, suffix, func):
		super(TransformerFunction, self).__init__()
		self.name = name
		self.new_name = f'{name}_{suffix}'
		self.func = func

	def __call__(self, *args, **kwargs):
		if len(args) == 0:
			replace = False if kwargs.get('replace') is None else kwargs['replace']
			use = [] if kwargs.get('use') is None else kwargs['use']
		elif len(args) < 2:
			replace = args[0]
			if len(args) == 1:
				use = [] if kwargs.get('use') is None else kwargs['use']
			else:
				use = args[1]
		else:
			print('Error')
			return

		if len(use) == 0:
			use = rep.names()

		for df_name in use:
			if self.name in rep.__getattr__(df_name).columns:
				df = rep.__getattr__(df_name)
				func_kwargs = {k: kwargs[k] for k in kwargs.keys() if k not in ['inplace', 'use']}
				df[self.new_name] = self.func(df, self.name, **func_kwargs)
				if replace:
					del df[self.name]


class FeatureTransformerBase(object):
	def __init__(self):
		super(FeatureTransformerBase, self).__init__()
	
	def __getattr__(self, name):
		if name.startswith('make'):
			items = name.split(sep='_')
			if len(items) == 3:
				return TransformerFunction(items[1],
				                           items[2],
				                           self.__getattribute__(f'to_{items[2]}'))
		else:
			return None


class DtFeatureTransformer(FeatureTransformerBase):
	@staticmethod
	def to_weekday(df, name):
		return np.floor(df[name] / (3600 * 24) - 1) % 7

	@staticmethod
	def to_dayhour(df, name):
		return np.floor(df[name] / 3600) % 24


class NumFeatureTransformer(FeatureTransformerBase):
	def __getattr__(self, name):
		if name.startswith('make'):
			items = name.split(sep='_')
			if len(items) == 3:
				return TransformerFunction(items[1],
				                           items[2],
				                           self.__getattribute__(f'to_{items[2]}'))
			elif len(items) == 4:
				return TransformerFunction(items[1],
				                           f'{items[2]}_{items[3]}',
				                           self.__getattribute__(f'to_{items[2]}'))
		else:
			return None

	@staticmethod
	def to_log(df, name):
		return df[name].apply(np.log)

	@staticmethod
	def to_mean(df, name, mean_name):
		return df[name] / df.groupby([mean_name])[name].transform('mean')

	@staticmethod
	def to_std(df, name, std_name):
		return df[name] / df.groupby([std_name])[name].transform('std')
