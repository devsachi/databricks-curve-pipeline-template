import os
import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.curve_formation.core.processor import CurveProcessor
from src.curve_formation.core.writer import CurveWriter
from src.utils.config_manager import ConfigManager
from src.utils.data_quality.validator import DataValidator

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return (SparkSession.builder
            .master("local[2]")
            .appName("curve_formation_integration_tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

@pytest.fixture(scope="session")
def config():
    """Load test configuration"""
    config_manager = ConfigManager()
    return config_manager.load_config("dev")

@pytest.fixture
def sample_market_data(spark):
    """Create sample market data for testing"""
    trade_date = datetime.now().strftime("%Y-%m-%d")
    
    # Interest rate data
    ir_data = [
        (trade_date, "SWAP", "1M", 0.02, "TRADER"),
        (trade_date, "SWAP", "3M", 0.025, "TRADER"),
        (trade_date, "SWAP", "6M", 0.03, "TRADER"),
        (trade_date, "SWAP", "1Y", 0.035, "TRADER")
    ]
    ir_df = spark.createDataFrame(
        ir_data,
        ["trade_date", "instrument_type", "tenor", "value", "source"]
    )
    
    # FX data
    fx_data = [
        (trade_date, "EUR/USD", "1M", 1.1000, -20, "TRADER"),
        (trade_date, "EUR/USD", "3M", 1.1000, -60, "TRADER"),
        (trade_date, "EUR/USD", "6M", 1.1000, -120, "TRADER"),
        (trade_date, "EUR/USD", "1Y", 1.1000, -240, "TRADER")
    ]
    fx_df = spark.createDataFrame(
        fx_data,
        ["trade_date", "currency_pair", "tenor", "spot_rate", "forward_points", "source"]
    )
    
    # Credit data
    credit_data = [
        (trade_date, "COMPANY1", "AA", "1Y", 50, "TRADER"),
        (trade_date, "COMPANY1", "AA", "2Y", 60, "TRADER"),
        (trade_date, "COMPANY1", "AA", "3Y", 70, "TRADER"),
        (trade_date, "COMPANY1", "AA", "5Y", 80, "TRADER")
    ]
    credit_df = spark.createDataFrame(
        credit_data,
        ["trade_date", "issuer", "rating", "tenor", "spread", "source"]
    )
    
    return {
        "IR": ir_df,
        "FX": fx_df,
        "CREDIT": credit_df
    }

@pytest.fixture
def curve_processor(config):
    """Create curve processor instance"""
    return CurveProcessor(config)

@pytest.fixture
def data_validator(config):
    """Create data validator instance"""
    return DataValidator()

def test_market_data_validation(sample_market_data, data_validator, config):
    """Test market data validation"""
    for asset_class, df in sample_market_data.items():
        validation_config = config["data_quality"][asset_class]
        assert data_validator.validate_market_data(df, validation_config)

def test_ir_curve_construction(sample_market_data, curve_processor, data_validator, config):
    """Test interest rate curve construction"""
    ir_curves = curve_processor.process(
        sample_market_data["IR"],
        asset_class="IR",
        trade_date=datetime.now()
    )
    
    # Validate curve output structure
    assert "USD_SWAP" in ir_curves
    curve_df = ir_curves["USD_SWAP"]
    
    # Check required columns
    required_cols = config["data_quality"]["IR_curves"]["required_columns"]
    assert all(col in curve_df.columns for col in required_cols)
    
    # Validate curve properties
    validation_config = config["data_quality"]["IR_curves"]
    assert data_validator.validate_market_data(curve_df, validation_config)

def test_fx_curve_construction(sample_market_data, curve_processor, data_validator, config):
    """Test FX curve construction"""
    fx_curves = curve_processor.process(
        sample_market_data["FX"],
        asset_class="FX",
        trade_date=datetime.now()
    )
    
    # Validate curve output structure
    assert "EUR/USD" in fx_curves
    curve_df = fx_curves["EUR/USD"]
    
    # Check required columns
    required_cols = config["data_quality"]["FX_curves"]["required_columns"]
    assert all(col in curve_df.columns for col in required_cols)
    
    # Validate curve properties
    validation_config = config["data_quality"]["FX_curves"]
    assert data_validator.validate_market_data(curve_df, validation_config)

def test_credit_curve_construction(sample_market_data, curve_processor, data_validator, config):
    """Test credit curve construction"""
    credit_curves = curve_processor.process(
        sample_market_data["CREDIT"],
        asset_class="CREDIT",
        trade_date=datetime.now()
    )
    
    # Validate curve output structure
    assert "COMPANY1_AA" in credit_curves
    curve_df = credit_curves["COMPANY1_AA"]
    
    # Check required columns
    required_cols = config["data_quality"]["CREDIT_curves"]["required_columns"]
    assert all(col in curve_df.columns for col in required_cols)
    
    # Validate curve properties
    validation_config = config["data_quality"]["CREDIT_curves"]
    assert data_validator.validate_market_data(curve_df, validation_config)

def test_curve_persistence(sample_market_data, curve_processor, config, tmp_path):
    """Test curve writing functionality"""
    # Process curves
    trade_date = datetime.now()
    ir_curves = curve_processor.process(
        sample_market_data["IR"],
        asset_class="IR",
        trade_date=trade_date
    )
    
    # Set up temporary storage location
    storage_path = str(tmp_path / "curves")
    test_config = dict(config)
    test_config["storage_root"] = storage_path
    
    # Write curves
    curve_writer = CurveWriter(test_config)
    curve_writer.write_curves(ir_curves, "IR", trade_date)
    
    # Verify written files
    assert os.path.exists(storage_path)
    
def test_end_to_end_pipeline(sample_market_data, config, spark):
    """Test complete pipeline execution"""
    # Initialize components
    curve_processor = CurveProcessor(config)
    data_validator = DataValidator()
    trade_date = datetime.now()
    
    try:
        # Validate input data
        for asset_class, df in sample_market_data.items():
            validation_config = config["data_quality"][asset_class]
            assert data_validator.validate_market_data(df, validation_config)
        
        # Process curves
        all_curves = {}
        for asset_class, df in sample_market_data.items():
            curves = curve_processor.process(df, asset_class, trade_date)
            all_curves[asset_class] = curves
            
            # Validate outputs
            validation_config = config["data_quality"][f"{asset_class}_curves"]
            for curve_name, curve_df in curves.items():
                assert data_validator.validate_market_data(curve_df, validation_config)
        
        assert len(all_curves) == len(sample_market_data)
        
    except Exception as e:
        pytest.fail(f"Pipeline execution failed: {str(e)}")
