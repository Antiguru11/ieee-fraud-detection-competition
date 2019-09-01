import config as cfg
from .data_frames_collection import DataFramesRepository

repository = DataFramesRepository(cfg.input_path, cfg.tmp_path)
