import os

base_path = 'e:\\Projects\\ml\\ieee-fraud-detection-competition\\'

input_path = os.path.join(base_path, 'input')
submissions_path = os.path.join(base_path, 'output')
models_path = os.path.join(base_path, 'models')
tmp_path = os.path.join(base_path, 'temp')

task_type = 0
target_col = 'isFraud'
target_vars = {'Fraud': 1, 'Not fraud': 0}

# train_transactions_path = os.path.join(input_path, 'train_transaction.csv')
# train_identify_path = os.path.join(input_path, 'train_identity.csv')
#
# test_transactions_path = os.path.join(input_path, 'test_transaction.csv')
# test_identify_path = os.path.join(input_path, 'test_identity.csv')
