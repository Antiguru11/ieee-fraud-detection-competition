import numpy as np
import pandas as pd

from data import repository as rep


train_dataframe = 'train'
test_dataframe = 'test'


def _get_dataframes():
	return rep.__getattr__(train_dataframe),\
	       rep.__getattr__(test_dataframe)


def to_weekday(name, new_name='weekday'):
	train, test = _get_dataframes()
	train[new_name] = train[name].apply(lambda x: np.floor(x / (3600 * 24) - 1) % 7)
	test[new_name] = test[name].apply(lambda x: np.floor(x / (3600 * 24) - 1) % 7)


def to_dayhour(name, new_name='dayhour'):
	train, test = _get_dataframes()
	train[new_name] = train[name].apply(lambda x: np.floor(x / 3600) % 24)
	test[new_name] = test[name].apply(lambda x: np.floor(x / 3600) % 24)


def to_log(name, new_name=None):
	if new_name is None:
		new_name = name + '_log'
	train, test = _get_dataframes()
	train[new_name] = train[name].map(np.log)
	test[new_name] = test[name].map(np.log)
