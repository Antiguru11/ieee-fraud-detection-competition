import os

base_path = 'd:\\Projects\\ml\\ieee-fraud-detection-competition\\'

input_path = os.path.join(base_path, 'input')
submissions_path = os.path.join(base_path, 'output')
models_path = os.path.join(base_path, 'models')
tmp_path = os.path.join(base_path, 'temp')

task_type = 0
target_col = 'isFraud'
target_vars = {'Fraud': 1, 'Not fraud': 0}

rep_index_col = {
	'test': 'TransactionID',
	'train': 'TransactionID'
}
