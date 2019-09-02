import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import config as cfg
from data import repository as rep


def cat_count(name, values=None, use=None, n_columns=3, **kwargs):
	plt.figure(**kwargs)
	if use is None or len(use) == 0:
		use = rep.names()

	if values is None or len(values) == 0:
		map_func = lambda x: True
	else:
		map_func = lambda x: x in values

	df_names = []
	df_names_with_target = []
	for df_name in use:
		df = rep.__getattr__(df_name)
		if name in df.columns:
			df_names.append(df_name)
			if cfg.target_col in df.columns:
				df_names_with_target.append(df_name)

	n_plots = len(df_names)
	if cfg.task_type == 0:
		n_plots += (len(cfg.target_vars) + 1) * len(df_names_with_target)
	n_rows = np.ceil(n_plots / n_columns)

	index = 1
	for df_name in df_names:
		df = rep.__getattr__(df_name)
		plt.subplot(n_rows, n_columns, index)
		ax = sns.countplot(x=name, data=df[df[name].apply(map_func)])
		ax.set_title('{0} in {1}'.format(name, df.name_))
		index += 1

	if cfg.task_type == 0:
		for df_name in df_names_with_target:
			for lbl, val in cfg.target_vars.items():
				df = rep.__getattr__(df_name)
				plt.subplot(n_rows, n_columns, index)
				ax = sns.countplot(x=name,
				                   hue=cfg.target_col,
				                   data=df[df[name].apply(map_func)][df[cfg.target_col] == val])
				ax.set_title('{0} in {1} - {2}'.format(name, df.name_, lbl))
				index += 1

		for df_name in df_names_with_target:
			df = rep.__getattr__(df_name)
			plt.subplot(n_rows, n_columns, index)
			ax = sns.countplot(x=name,
			                   hue=cfg.target_col,
			                   data=df[df[name].apply(map_func)])
			ax.set_title('{0} in {1} by {2}'.format(name, df.name_, cfg.target_col))
			index += 1

	plt.show()
