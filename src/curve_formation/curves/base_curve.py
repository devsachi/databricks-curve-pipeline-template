from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from src.utils.data_quality.validator import DataQualityValidator
from src.curve_formation.business_logic import DayCountConvention, Calendar

class BaseCurve(ABC):
    def __init__(self, config):
        self.config = config
        self.day_count = DayCountConvention(config.curve_params.default_day_count)
        self.calendar = Calendar(config.curve_params.business_calendar)
        self.dq_validator = DataQualityValidator(config)
    
    @abstractmethod
    def calculate(self, input_df: DataFrame) -> DataFrame:
        """Calculate the specific curve"""
        pass
        
    def _prepare_input(self, input_df: DataFrame) -> DataFrame:
        """Common input preparation logic"""
        self.dq_validator.validate_base_input(input_df)
        return input_df.dropDuplicates()
