import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import config
from data import repository as rep


def _get_data_frames(col_name):
	dfs = []
	for df in rep:
		if col_name in df.columns:
			dfs.append(df)
	return dfs


def cat_count(col_name, figsize, nrows, ncols):
	dfs = _get_data_frames(col_name)
	plt.figure(figsize=figsize)
	index = 1
	for df in dfs:
		plt.subplot(nrows, ncols, index)
		ax = sns.countplot(x=col_name,
		                   data=df)
		ax.set_title('{0} in {1}'.format(col_name, df.name_))
		index += 1

	for df in dfs:
		if config.task_type == 'classification' and \
		   config.target_col in df.columns:
			for lbl, val in config.target_vars.items():
				plt.subplot(nrows, ncols, index)
				ax = sns.countplot(x=col_name,
				                   data=df[df[config.target_col] == val])
				ax.set_title('{0} in {1} - {2}'.format(col_name, df.name_, lbl))
				index += 1
			plt.subplot(nrows, ncols, index)
			ax = sns.countplot(x=col_name,
			                   hue=config.target_col,
			                   data=df)
			ax.set_title('{0} in {1} by {2}'.format(col_name, df.name_, config.target_col))
			index += 1

	plt.show()
