import numpy as np
import pandas as pd

from data import repository as rep


train_dataframe = 'train'
test_dataframe = 'test'


def _get_dataframes():
	return rep.__getattr__(train_dataframe), rep.__getattr__(test_dataframe)


def _update_dataframes(train, test):
	rep[train_dataframe]._data = train
	rep[test_dataframe]._data = test


def to_weekday(col_name, new_col_name='weekday'):
	train, test = _get_dataframes()
	train[new_col_name] = train[col_name].apply(lambda x: np.floor(x / (3600 * 24) - 1) % 7)
	test[new_col_name] = test[col_name].apply(lambda x: np.floor(x / (3600 * 24) - 1) % 7)
	#_update_dataframes(train, test)


def to_dayhour(col_name, new_col_name='dayhour'):
	train, test = _get_dataframes()
	train[new_col_name] = train[col_name].apply(lambda x: np.floor(x / 3600) % 24)
	test[new_col_name] = test[col_name].apply(lambda x: np.floor(x / 3600) % 24)
	#_update_dataframes(train, test)


def to_log(col_name, new_col_name=None):
	if new_col_name is None:
		new_col_name = col_name + '_log'
	train, test = _get_dataframes()
	train[new_col_name] = train[col_name].map(np.log)
	test[new_col_name] = test[col_name].map(np.log)
	#_update_dataframes(train, test)
