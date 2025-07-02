# %% [markdown]
# # Comparative Analysis of ONA, CMA, and DEMA Algorithms for Aethalometer Data

# %% [markdown]
# ## 1. Introduction and Background
# 
# This notebook implements and compares three post-processing algorithms for micro-aethalometer data:
#   
# 1. **Optimized Noise-reduction Algorithm (ONA)**: A method described by Hagler et al. (2011) that adaptively time-averages BC data based on incremental light attenuation (ΔATN).
# 
# 2. **Centered Moving Average (CMA)**: A smoothing technique that incorporates data points both before and after each measurement to reduce noise while preserving microenvironmental characteristics.
# 
# 3. **Double Exponentially Weighted Moving Average (DEMA)**: A smoothing approach that reduces noise-induced artifacts while limiting lag, especially useful for source apportionment calculations.
#  
# Both CMA and DEMA have been shown to outperform ONA for newer dual-spot aethalometers in recent research by Liu et al. (2021) and Mendoza et al. (2024).
# 

# %% [markdown]
# ## 2. Import Libraries

# %%
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import seaborn as sns
import numba
from IPython.display import display

# Set plot style
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12

# %% [markdown]
# 

# %% [markdown]
# ## 3. Load and Explore the Data
#  
# First, let's load the Aethalometer data and examine its structure.

# %%
# Define the file path - replace with your actual file path
file_path = "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/Aethelometry Data/Jacros_MA350_1-min_2022-2024_Cleaned.csv"  # Replace with your file path

# Check if pyarrow is available for optimal performance
try:
    import pyarrow
    use_pyarrow = True
except ImportError:
    print("Warning: pyarrow not found, using slower pandas conversion method")
    use_pyarrow = False

# Load the data with Polars
pl_data = pl.read_csv(file_path)

# Display basic information about the dataset
print(f"Dataset shape: {pl_data.shape}")
print("\nColumn names:")
print(pl_data.columns)

# Display the first few rows
print("\nFirst few rows of the dataset:")
display(pl_data.head())

# Create a table to collect wavelength statistics
wavelength_stats = []
wavelengths = ['UV', 'Blue', 'Green', 'Red', 'IR']

# Check for the presence of BC columns for each wavelength
for wavelength in wavelengths:
    bc_col = f"{wavelength} BCc"
    atn_col = f"{wavelength} ATN1"
    
    if bc_col in pl_data.columns:
        # Using Polars expressions for statistics
        stats = {}
        stats["Wavelength"] = wavelength
        
        # Get BC range
        bc_range = pl_data.select([
            pl.col(bc_col).min().alias("bc_min"),
            pl.col(bc_col).max().alias("bc_max")
        ])
        stats["BC Min (ng/m³)"] = int(bc_range[0, "bc_min"])
        stats["BC Max (ng/m³)"] = int(bc_range[0, "bc_max"])
        
        # Count negative values
        neg_count = pl_data.filter(pl.col(bc_col) < 0).height
        neg_percentage = (neg_count / pl_data.height) * 100
        stats["Negative BC Count"] = neg_count
        stats["Negative BC (%)"] = round(neg_percentage, 2)
        
        # Get ATN range if available
        if atn_col in pl_data.columns:
            atn_range = pl_data.select([
                pl.col(atn_col).min().alias("atn_min"),
                pl.col(atn_col).max().alias("atn_max")
            ])
            stats["ATN Min"] = float(atn_range[0, "atn_min"])
            stats["ATN Max"] = float(atn_range[0, "atn_max"])
        else:
            stats["ATN Min"] = None
            stats["ATN Max"] = None
        
        wavelength_stats.append(stats)
    else:
        print(f"\nWarning: {wavelength} data columns not found")

# Create a DataFrame from the statistics and explicitly set the data types
if wavelength_stats:
    stats_df = pl.DataFrame(wavelength_stats)
    
    # Ensure proper data types for better display
    stats_df = stats_df.with_columns([
        pl.col("Wavelength").cast(pl.Utf8),
        pl.col("BC Min (ng/m³)").cast(pl.Int64),
        pl.col("BC Max (ng/m³)").cast(pl.Int64),
        pl.col("Negative BC Count").cast(pl.Int64),
        pl.col("Negative BC (%)").cast(pl.Float64).round(2),
        pl.col("ATN Min").cast(pl.Float64).round(3),
        pl.col("ATN Max").cast(pl.Float64).round(3)
    ])
    
    print("\nWavelength Statistics:")
    
    # Convert to pandas for better display formatting
    import pandas as pd
    pd_stats = stats_df.to_pandas()
    
    # Format the display to remove quotes
    pd.set_option('display.precision', 2)
    display(pd_stats)

# Check the time resolution
if 'Timebase (s)' in pl_data.columns:
    timebase = pl_data.select(pl.col('Timebase (s)')).row(0)[0]
    print(f"\nInstrument timebase: {timebase} seconds")
else:
    print("\nTimebase column not found")

# Convert to pandas for compatibility with the rest of the notebook
# Use pyarrow if available for better performance
if use_pyarrow:
    data = pl_data.to_pandas()
else:
    # Fallback method if pyarrow is not available
    import pandas as pd
    data = pd.DataFrame({col: pl_data[col].to_numpy() for col in pl_data.columns})

# %% [markdown]
# ## 4. ONA Algorithm Implementation
#   
# The Optimized Noise-reduction Algorithm (ONA) adaptively time-averages BC data based on incremental light attenuation (ΔATN).

# %%
@numba.jit(nopython=True)
def _numba_nanmean(arr_slice):
    """
    Numba-compatible nanmean.
    """
    finite_sum = 0.0
    finite_count = 0
    for x in arr_slice:
        if not np.isnan(x): # np.isnan is Numba compatible
            finite_sum += x
            finite_count += 1
    if finite_count == 0:
        return np.nan
    return finite_sum / finite_count

@numba.jit(nopython=True)
def process_ona_segment_numba_core(
    bc_col_np: np.ndarray,
    atn_col_np: np.ndarray,
    delta_atn_min: float,
    num_rows: int
):
    """
    Core ONA algorithm implemented for Numba.
    Operates on NumPy arrays for a single data segment.
    This logic closely mirrors the MATLAB ONA windowing approach within a segment.
    """
    # Initialize output arrays
    smoothed_col_np = bc_col_np.copy() # Start with original BC values
    points_col_np = np.ones(num_rows, dtype=np.int64) # Default points averaged is 1

    # If only one point (or none) in the segment, it's effectively averaged with itself.
    # smoothed_col_np is already a copy, and points_col_np is already 1.
    if num_rows <= 1:
        return smoothed_col_np, points_col_np

    j = 0 # Current starting index of the averaging window (0-based)
    while j < num_rows:
        cur_atn = atn_col_np[j]

        # Find the end of the window (exclusive index).
        # The window includes point j. All subsequent points k in the window
        # must satisfy: atn_col_np[k] <= cur_atn + delta_atn_min
        # and k must be < num_rows (within the current segment).
        window_end_exclusive = j + 1
        while window_end_exclusive < num_rows:
            if atn_col_np[window_end_exclusive] > cur_atn + delta_atn_min:
                break # End of window found
            window_end_exclusive += 1
        
        # The current window for averaging is from index j to window_end_exclusive-1
        slice_bc_np = bc_col_np[j:window_end_exclusive]
        
        avg_bc = _numba_nanmean(slice_bc_np)
        num_points_in_window = window_end_exclusive - j # Length of the slice

        # Apply the averaged BC value and points count to all rows in the window
        for k_idx in range(j, window_end_exclusive):
            smoothed_col_np[k_idx] = avg_bc
            points_col_np[k_idx] = num_points_in_window
            
        # Move to the start of the next potential window
        j = window_end_exclusive
            
    return smoothed_col_np, points_col_np

# Assuming process_ona_segment_numba_core and _numba_nanmean are defined above

def process_ona_segment_numba_wrapper(
    pdf: pd.DataFrame,
    bc_col: str,
    atn_col: str,
    points_col: str,
    smoothed_col: str,
    delta_atn_min: float
) -> pd.DataFrame:
    """
    Wrapper to run the ONA algorithm on one pandas segment using the Numba core.
    Returns the pandas DataFrame with updated points_col and smoothed_col columns.
    """
    # Initialize the columns with default values.
    # Numba core will operate on copies, so these initializations set the base.
    if points_col not in pdf.columns:
        pdf[points_col] = 1
    else:
        pdf.loc[:, points_col] = 1 # Use .loc to avoid SettingWithCopyWarning

    if smoothed_col not in pdf.columns:
        pdf[smoothed_col] = pdf[bc_col].copy()
    else:
        pdf.loc[:, smoothed_col] = pdf[bc_col].copy()


    if len(pdf) <= 1:
        # If the segment is very short, ensure original values (or copy) and 1 point averaged is returned
        # This also ensures the columns exist even if Numba core is skipped.
        pdf.loc[:, smoothed_col] = pdf[bc_col].copy() # Ensure it's a copy of current segment's BC
        pdf.loc[:, points_col] = 1
        return pdf

    # Convert relevant pandas Series to NumPy arrays for Numba
    # Ensure correct dtype. float64 is generally safe for numerical stability.
    bc_col_np = pdf[bc_col].to_numpy(dtype=np.float64, na_value=np.nan)
    atn_col_np = pdf[atn_col].to_numpy(dtype=np.float64, na_value=np.nan)
    
    num_rows = len(pdf)

    # Call the Numba-optimized core function
    smoothed_result_np, points_result_np = process_ona_segment_numba_core(
        bc_col_np, atn_col_np, delta_atn_min, num_rows
    )

    # Assign results back to the pandas DataFrame using .loc to ensure modification
    pdf.loc[:, smoothed_col] = smoothed_result_np
    pdf.loc[:, points_col] = points_result_np
    
    return pdf

# Assuming process_ona_segment_numba_wrapper is defined above

def apply_ona_polars_numba(
    data: pl.DataFrame,
    wavelength: str = "Blue",
    delta_atn_min: float = 0.05
) -> pl.DataFrame:
    """
    Apply the Optimized Noise-reduction Algorithm using Polars for segmentation
    and a Numba-accelerated Pandas UDF for the core ONA processing.
    Includes schema overrides for consistent concatenation.
    """
    # Column names
    bc_col = f"{wavelength} BCc"
    atn_col = f"{wavelength} ATN1"
    points_col = f"{wavelength}_points_averaged"
    smoothed_col = f"{wavelength}_BC_ONA"

    # --- Boilerplate: Check if required columns exist ---
    if atn_col not in data.columns or bc_col not in data.columns:
        print(f"Warning: Required columns ({atn_col}, {bc_col}) not found for ONA on wavelength {wavelength}. Skipping ONA.")
        output_data = data.clone()
        if points_col not in output_data.columns:
            output_data = output_data.with_columns(pl.lit(1).cast(pl.Int64).alias(points_col))
        if smoothed_col not in output_data.columns:
            if bc_col in output_data.columns: # Check if bc_col exists before trying to alias from it
                output_data = output_data.with_columns(pl.col(bc_col).alias(smoothed_col))
            else: # If bc_col also doesn't exist, fill with nulls
                output_data = output_data.with_columns(pl.lit(None, dtype=pl.Float64).alias(smoothed_col))
        return output_data

    # --- Segmentation Logic (using Polars) ---
    # This part is different from MATLAB's manual `filtchange` array construction,
    # but it's a robust way to define segments for processing.
    df = data.clone()
    df = df.with_columns(
        pl.col(atn_col).diff().abs().alias("ΔATN") # First value will be null
    )
    # A new segment starts if ΔATN > 30 OR if ΔATN is null (which handles the very first row)
    df = df.with_columns(
        (
            (pl.col("ΔATN") > 30) | pl.col("ΔATN").is_null()
        )
        .fill_null(True) # Ensure the is_null() condition on the first row creates a segment
        .cast(pl.Int32)
        .cum_sum() # Create a unique ID for each segment
        .alias("segment_id")
    )
    actual_filter_changes = df.filter(pl.col("ΔATN") > 30).height # Count actual breaks
    print(f"Number of actual filter changes (ΔATN > 30) detected for {wavelength}: {actual_filter_changes}")
    
    # Initialize output columns in the main DataFrame before splitting into segments
    # The values will be correctly populated by the wrapper.
    df = df.with_columns([
        pl.lit(1).cast(pl.Int64).alias(points_col),
        pl.col(bc_col).cast(pl.Float64).alias(smoothed_col) 
    ])
    
    # Determine sort column for ensuring data order
    if "Time (UTC)" in df.columns:
        sort_col = "Time (UTC)"
    elif "_idx" in df.columns: # If an _idx column was previously added
        sort_col = "_idx"
    else: # Add a row count if no other suitable sort column exists
        df = df.with_row_count("_idx")
        sort_col = "_idx"

    # --- Process Each Segment ---
    processed_segments_list = []
    # Group by segment_id and apply the processing.
    # Using group_by().apply() can be more idiomatic in Polars/Pandas if the UDF is adapted.
    # However, explicit loop is fine and perhaps clearer for this translation.
    unique_segment_ids = df.get_column("segment_id").unique().sort()


    for seg_id_val in unique_segment_ids:
        segment_pl = df.filter(pl.col("segment_id") == seg_id_val)
        segment_pl = segment_pl.sort(sort_col) # Ensure order within segment
        
        # Convert segment to Pandas DataFrame for the Numba wrapper
        pd_segment = segment_pl.to_pandas()
        
        pd_processed_segment = process_ona_segment_numba_wrapper(
            pd_segment, bc_col, atn_col, points_col, smoothed_col, delta_atn_min
        )

        # Define schema overrides for consistency when converting back to Polars
        # This is important because operations in Pandas can sometimes alter dtypes.
        schema_overrides_dict = {
            points_col: pl.Int64,    
            smoothed_col: pl.Float64 
        }
        
        # Ensure any other columns that might be affected maintain their type (example below)
        problematic_bc_pass_through_cols = [
            'UV BC1', 'UV BC2', 'UV BCc', 'Blue BC1', 'Blue BC2', # 'Blue BCc' is bc_col
            'Green BC1', 'Green BC2', 'Green BCc', 'Red BC1', 'Red BC2', 'Red BCc',
            'IR BC1', 'IR BC2', 'IR BCc'
        ]
        for col_to_override in problematic_bc_pass_through_cols:
            if col_to_override in pd_processed_segment.columns and col_to_override != bc_col : # bc_col is already handled by smoothed_col if they are same initially
                 if col_to_override != smoothed_col: # smoothed_col already in schema_overrides_dict
                    schema_overrides_dict[col_to_override] = pl.Float64

        pl_processed_segment = pl.from_pandas(
            pd_processed_segment,
            schema_overrides=schema_overrides_dict
        )
        processed_segments_list.append(pl_processed_segment)

    # --- Combine Processed Segments ---
    if processed_segments_list:
        try:
            result_df = pl.concat(processed_segments_list, how="vertical_relaxed") # Use vertical_relaxed for more robustness
        except pl.SchemaError as e: # Should be less likely with vertical_relaxed and schema_overrides
            print(f"Polars concat failed even with vertical_relaxed: {e}")
            raise 

        result_df = result_df.sort(sort_col) # Restore original overall order
        
        # Drop temporary columns
        temp_cols_to_drop = ["ΔATN", "segment_id"]
        if sort_col == "_idx" and "_idx" not in data.columns : # If _idx was added by this function
            temp_cols_to_drop.append("_idx")
        
        final_cols_to_drop = [col for col in temp_cols_to_drop if col in result_df.columns]
        if final_cols_to_drop:
            result_df = result_df.drop(final_cols_to_drop)
    else: # Should not happen if df has rows
        result_df = df.clone() # Start from a clone of the (potentially _idx added) df
        temp_cols_to_drop = ["ΔATN", "segment_id"]
        if sort_col == "_idx" and "_idx" not in data.columns:
             temp_cols_to_drop.append("_idx")
        final_cols_to_drop = [col for col in temp_cols_to_drop if col in result_df.columns]
        if final_cols_to_drop:
            result_df = result_df.drop(final_cols_to_drop)


    return result_df

# %% [markdown]
# ## 5. CMA Algorithm Implementation
#   
# The Centered Moving Average is a smoothing technique that uses data points both before and after each measurement.

# %%
def apply_cma_polars(data, wavelength='Blue', window_size=None):
    """
    Apply the Centered Moving Average algorithm to Aethalometer data (Polars implementation)
    
    Parameters:
    -----------
    data : polars.DataFrame
        DataFrame containing Aethalometer data
    wavelength : str
        Which wavelength to process ('UV', 'Blue', 'Green', 'Red', 'IR')
    window_size : int or None
        Size of the moving average window (must be odd). If None, 
        will use a default based on the data's timebase
        
    Returns:
    --------
    data_smoothed : polars.DataFrame
        DataFrame with the original data plus additional columns for smoothed BC
    """
    # Create a copy of the input dataframe
    data_smoothed = data.clone()
    
    # Identify the column for BC values based on wavelength
    bc_col = f"{wavelength} BCc"
    smoothed_bc_col = f"{wavelength}_BC_CMA"
    
    # Determine window size if not specified
    if window_size is None:
        if 'Timebase (s)' in data_smoothed.columns:
            timebase = data_smoothed.select(pl.col('Timebase (s)')).row(0)[0]
            if timebase == 1:
                window_size = 11  # 11 seconds for 1-second data
            elif timebase == 5:
                window_size = 5   # 25 seconds for 5-second data
            elif timebase == 60:
                window_size = 3   # 3 minutes for 1-minute data
            else:
                window_size = 5   # Default for other timebases
        else:
            window_size = 5       # Default if timebase is unknown
    
    # Make sure window_size is odd
    if window_size % 2 == 0:
        window_size += 1
    
    print(f"Using window size of {window_size} for CMA on {wavelength}")
    
    # Apply rolling mean with center=True
    data_smoothed = data_smoothed.with_columns(
        pl.col(bc_col).rolling_mean(
            window_size=window_size,
            center=True,
            min_samples=1  # Updated from min_periods to min_samples
        ).alias(smoothed_bc_col)
    )
    
    return data_smoothed

# %% [markdown]
# ## 6. DEMA Algorithm Implementation
#  
# The Double Exponentially Weighted Moving Average applies additional smoothing to an EMA to reduce noise while limiting lag.

# %%
def apply_dema_polars(data, wavelength='Blue', alpha=None):
    """
    Apply the Double Exponentially Weighted Moving Average algorithm (Polars implementation)
    
    Parameters:
    -----------
    data : polars.DataFrame
        DataFrame containing Aethalometer data
    wavelength : str
        Which wavelength to process ('UV', 'Blue', 'Green', 'Red', 'IR')
    alpha : float
        Smoothing parameter (between 0 and 1)
        For 60s data, 0.125 approximates a 15-minute smoothing window
        
    Returns:
    --------
    data_smoothed : polars.DataFrame
        DataFrame with the original data plus additional columns for smoothed BC
    """
    # Create a copy of the input dataframe
    data_smoothed = data.clone()
    
    # Identify the column for BC values based on wavelength
    bc_col = f"{wavelength} BCc"
    ema_col = f"{wavelength}_EMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    # Set the smoothing parameter based on timebase if not explicitly provided
    if 'Timebase (s)' in data_smoothed.columns:
        timebase = data_smoothed.select(pl.col('Timebase (s)')).row(0)[0]
        if alpha is None:
            # Use formula 2/(N+1) where N is the desired smoothing period
            if timebase == 1:
                # Default to approximate 5-minute window for 1-second data
                N = 300 / timebase
            elif timebase == 5:
                # Default to approximate 5-minute window for 5-second data
                N = 300 / timebase
            elif timebase == 60:
                # Default to approximate 15-minute window for 60-second data
                N = 900 / timebase
            else:
                N = 15  # Default for other timebases
                
            alpha = 2 / (N + 1)
    else:
        # Default alpha if timebase is unknown
        if alpha is None:
            alpha = 0.125
    
    print(f"Using alpha of {alpha:.4f} for DEMA on {wavelength}")
    
    # First EMA calculation
    data_smoothed = data_smoothed.with_columns(
        pl.col(bc_col).ewm_mean(alpha=alpha, adjust=False).alias(ema_col)
    )
    
    # Second EMA calculation (EMA of EMA)
    data_smoothed = data_smoothed.with_columns(
        pl.col(ema_col).ewm_mean(alpha=alpha, adjust=False).alias("ema_of_ema")
    )
    
    # Calculate DEMA: (2 * EMA) - EMA(EMA)
    data_smoothed = data_smoothed.with_columns(
        (2 * pl.col(ema_col) - pl.col("ema_of_ema")).alias(dema_col)
    )
    
    # Drop the temporary ema_of_ema column
    data_smoothed = data_smoothed.drop("ema_of_ema")
    
    return data_smoothed

# %% [markdown]
# ## 7. Apply All Processing Methods to Data

# %%
# Create separate dataframes for each method using Polars
pl_processed_data_raw = pl_data.clone()
pl_processed_data_ona = pl_data.clone()
pl_processed_data_cma = pl_data.clone()
pl_processed_data_dema = pl_data.clone()

# Set the minimum change in attenuation (ΔATN) for ONA
delta_atn_min = 0.05  # Default value from the Hagler paper

# Process each wavelength with all methods
for wavelength in wavelengths:
    bc_col = f"{wavelength} BCc"
    atn_col = f"{wavelength} ATN1"
    
    if bc_col in pl_processed_data_raw.columns: # Check if the primary data column exists
        # Apply ONA if ATN column also exists (required for ONA)
        if atn_col in pl_processed_data_raw.columns: # Ensure ATN column exists in the source data
            print(f"\nApplying ONA to {wavelength} wavelength data...")
            # VVV This is the updated line VVV
            pl_processed_data_ona = apply_ona_polars_numba(pl_processed_data_ona, wavelength, delta_atn_min)
        else:
            print(f"\nSkipping ONA for {wavelength}: ATN column ({atn_col}) not found.")
        
        # Apply CMA
        print(f"\nApplying CMA to {wavelength} wavelength data...")
        pl_processed_data_cma = apply_cma_polars(pl_processed_data_cma, wavelength) # Assuming this function is defined
        
        # Apply DEMA
        print(f"\nApplying DEMA to {wavelength} wavelength data...")
        pl_processed_data_dema = apply_dema_polars(pl_processed_data_dema, wavelength) # Assuming this function is defined

# Convert to pandas for compatibility with downstream functions if needed
processed_data_raw = pl_processed_data_raw.to_pandas()
processed_data_ona = pl_processed_data_ona.to_pandas()
processed_data_cma = pl_processed_data_cma.to_pandas()
processed_data_dema = pl_processed_data_dema.to_pandas()

# %% [markdown]
# ## 8. Calculate Source Apportionment
#  
# Following the recommendations from recent research, we'll implement source apportionment calculations using the Aethalometer model for each processing method.

# %%
def calculate_source_apportionment(data, aae_wb=2.0, aae_ff=1.0):
    """
    Calculate source apportionment using the Aethalometer Model
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame with BC data at blue and IR wavelengths
    aae_wb : float
        Absorption Ångström Exponent for wood burning (default 2.0)
    aae_ff : float
        Absorption Ångström Exponent for fossil fuel (default 1.0)
    
    Returns:
    --------
    data_sa : pandas.DataFrame
        DataFrame with additional source apportionment columns
    """
    # Create a copy of the input dataframe
    data_sa = data.copy()
    
    # Convert to Polars for faster calculations
    pl_data = pl.from_pandas(data[['Blue BCc', 'IR BCc']])
    
    # Constants
    mac_blue = 10.12  # m²/g at 470nm
    mac_ir = 7.77     # m²/g at 880nm
    c_ref = 1.3       # Multiple scattering enhancement factor
    wavelength_ratio = 470 / 880
    
    # Calculate absorption coefficients
    pl_data = pl_data.with_columns(
        (pl.col('Blue BCc') * mac_blue / c_ref).alias('Babs_Blue'),
        (pl.col('IR BCc') * mac_ir / c_ref).alias('Babs_IR')
    )
    
    # Calculate absorption coefficients for wood burning and fossil fuel at IR wavelength
    pl_data = pl_data.with_columns(
        ((pl.col('Babs_Blue') - pl.col('Babs_IR') * (wavelength_ratio ** (-aae_wb))) / 
        ((wavelength_ratio ** (-aae_ff)) - (wavelength_ratio ** (-aae_wb)))).alias('Babs_FF_IR')
    )
    
    pl_data = pl_data.with_columns(
        (pl.col('Babs_IR') - pl.col('Babs_FF_IR')).alias('Babs_WB_IR')
    )
    
    # Calculate BB% (biomass burning percentage)
    pl_data = pl_data.with_columns(
        (100 * pl.col('Babs_WB_IR') / pl.col('Babs_IR')).alias('BB_Percent')
    )
    
    # Handle infinity and NaN values
    pl_data = pl_data.with_columns(
        pl.when(pl.col('BB_Percent').is_infinite() | pl.col('BB_Percent').is_nan())
        .then(pl.lit(0))
        .otherwise(pl.col('BB_Percent'))
        .alias('BB_Percent')
    )
    
    # Calculate BC from wood burning and fossil fuel
    pl_data = pl_data.with_columns(
        (pl.col('IR BCc') * pl.col('BB_Percent') / 100).alias('BC_WB'),
        (pl.col('IR BCc') * (100 - pl.col('BB_Percent')) / 100).alias('BC_FF')
    )
    
    # Convert back to pandas DataFrame
    for col in pl_data.columns:
        if col not in data_sa.columns:
            data_sa[col] = pl_data[col].to_numpy()
    
    return data_sa

# %%
# Calculate source apportionment for raw data
print("\nCalculating source apportionment for raw data...")
processed_data_raw_sa = calculate_source_apportionment(processed_data_raw)

# Calculate source apportionment for ONA-processed data
if 'Blue_BC_ONA' in processed_data_ona.columns and 'IR_BC_ONA' in processed_data_ona.columns:
    print("\nCalculating source apportionment for ONA-processed data...")
    processed_data_ona_sa = processed_data_ona.copy()
    # Use the ONA-processed data for source apportionment
    processed_data_ona_sa['Blue BCc'] = processed_data_ona['Blue_BC_ONA']
    processed_data_ona_sa['IR BCc'] = processed_data_ona['IR_BC_ONA']
    processed_data_ona_sa = calculate_source_apportionment(processed_data_ona_sa)
else:
    processed_data_ona_sa = processed_data_raw_sa.copy()
    print("\nWarning: ONA-processed data not available for both Blue and IR wavelengths, using raw data for source apportionment")

# Calculate source apportionment for CMA-processed data
print("\nCalculating source apportionment for CMA-processed data...")
processed_data_cma_sa = processed_data_cma.copy()
# Use the CMA-processed data for source apportionment
processed_data_cma_sa['Blue BCc'] = processed_data_cma['Blue_BC_CMA']
processed_data_cma_sa['IR BCc'] = processed_data_cma['IR_BC_CMA']
processed_data_cma_sa = calculate_source_apportionment(processed_data_cma_sa)

# Calculate source apportionment for DEMA-processed data
print("\nCalculating source apportionment for DEMA-processed data...")
processed_data_dema_sa = processed_data_dema.copy()
# Use the DEMA-processed data for source apportionment
processed_data_dema_sa['Blue BCc'] = processed_data_dema['Blue_BC_DEMA']
processed_data_dema_sa['IR BCc'] = processed_data_dema['IR_BC_DEMA'] 
processed_data_dema_sa = calculate_source_apportionment(processed_data_dema_sa)

# %% [markdown]
# ## 9. Evaluate Processing Performance
#   
# Now let's evaluate how well each processing method performed and compare them.

# %%
def evaluate_processing(data_raw, data_processed, raw_col, processed_col):
    """
    Evaluate the performance of a processing algorithm
    
    Parameters:
    -----------
    data_raw : pandas.DataFrame
        DataFrame with original data
    data_processed : pandas.DataFrame
        DataFrame with processed data
    raw_col : str
        Column name for raw data
    processed_col : str
        Column name for processed data
        
    Returns:
    --------
    metrics : dict
        Dictionary of performance metrics
    """
    # Convert to Polars for faster calculations
    pl_raw = pl.from_pandas(data_raw[[raw_col]])
    pl_processed = pl.from_pandas(data_processed[[processed_col]])
    
    # 1. Reduction of negatives
    numneg_raw = pl_raw.filter(pl.col(raw_col) < 0).height / pl_raw.height
    numneg_processed = pl_processed.filter(pl.col(processed_col) < 0).height / pl_processed.height
    
    print(f"Fraction of negative values in raw data: {numneg_raw:.4f}")
    print(f"Fraction of negative values after processing: {numneg_processed:.4f}")
    
    if numneg_raw > 0:
        reduction = (numneg_raw - numneg_processed)/numneg_raw
        print(f"Reduction in negative values: {reduction:.4f} ({reduction*100:.1f}%)")
    else:
        reduction = 0
        print("No negative values in raw data to reduce")
    
    # 2. Reduction of noise (average absolute difference between consecutive points)
    # This is a bit complex with Polars, let's use Pandas/NumPy for this part
    temp_raw = np.abs(np.diff(data_raw[raw_col].values))
    temp_processed = np.abs(np.diff(data_processed[processed_col].values))
    
    noise_raw = np.nanmean(temp_raw)
    noise_processed = np.nanmean(temp_processed)
    
    print(f"Noise in raw data: {noise_raw:.1f} ng/m³")
    print(f"Noise in processed data: {noise_processed:.1f} ng/m³")
    print(f"Noise reduction factor: {noise_raw/noise_processed:.1f}x")
    
    # 3. Calculate correlation with raw data
    # Create a combined Polars DataFrame
    pl_combined = pl.from_pandas(pd.DataFrame({
        'raw': data_raw[raw_col],
        'processed': data_processed[processed_col]
    }))
    
    correlation = pl_combined.select(
        pl.corr('raw', 'processed').alias('correlation')
    )[0, 0]
    
    print(f"Correlation with raw data: {correlation:.4f}")
    
    return {
        'negative_original': numneg_raw,
        'negative_processed': numneg_processed,
        'negative_reduction': reduction,
        'noise_original': noise_raw,
        'noise_processed': noise_processed,
        'noise_reduction': noise_raw/noise_processed,
        'correlation': correlation
    }

# %%
# Place this after Section 8 or at the start of Section 9

print("\nCalculating Source Apportionment Stability Metrics (Std Dev of BB%)...")
sa_stability_metrics = {} # Initialize the dictionary

# Raw data
if 'processed_data_raw_sa' in locals() and 'BB_Percent' in processed_data_raw_sa.columns:
    sa_stability_metrics['Raw'] = processed_data_raw_sa['BB_Percent'].std()
else:
    sa_stability_metrics['Raw'] = np.nan
    print("Warning: Raw SA data for BB% std dev not found.")

# ONA processed data
if 'processed_data_ona_sa' in locals() and 'BB_Percent' in processed_data_ona_sa.columns:
    sa_stability_metrics['ONA'] = processed_data_ona_sa['BB_Percent'].std()
else:
    sa_stability_metrics['ONA'] = np.nan
    print("Warning: ONA SA data for BB% std dev not found.")

# CMA processed data
if 'processed_data_cma_sa' in locals() and 'BB_Percent' in processed_data_cma_sa.columns:
    sa_stability_metrics['CMA'] = processed_data_cma_sa['BB_Percent'].std()
else:
    sa_stability_metrics['CMA'] = np.nan
    print("Warning: CMA SA data for BB% std dev not found.")
    
# DEMA processed data
if 'processed_data_dema_sa' in locals() and 'BB_Percent' in processed_data_dema_sa.columns:
    sa_stability_metrics['DEMA'] = processed_data_dema_sa['BB_Percent'].std()
else:
    sa_stability_metrics['DEMA'] = np.nan
    print("Warning: DEMA SA data for BB% std dev not found.")

print("\nSource Apportionment Stability Metrics (Std Dev of BB%):")
for method, std_dev in sa_stability_metrics.items():
    if not np.isnan(std_dev):
        print(f"  {method}: {std_dev:.2f}")
    else:
        print(f"  {method}: N/A")
        
# Evaluate each wavelength and method
summary_metrics = {}

for wavelength in ['Blue', 'IR']:
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    if bc_col in data.columns:
        print(f"\n===== Performance Evaluation for {wavelength} Wavelength =====")
        
        # Evaluate ONA if available
        if ona_col in processed_data_ona.columns:
            print(f"\nEvaluating ONA for {wavelength} wavelength:")
            summary_metrics[f"{wavelength}_ONA"] = evaluate_processing(data, processed_data_ona, bc_col, ona_col)
        
        # Evaluate CMA
        print(f"\nEvaluating CMA for {wavelength} wavelength:")
        summary_metrics[f"{wavelength}_CMA"] = evaluate_processing(data, processed_data_cma, bc_col, cma_col)
        
        # Evaluate DEMA
        print(f"\nEvaluating DEMA for {wavelength} wavelength:")
        summary_metrics[f"{wavelength}_DEMA"] = evaluate_processing(data, processed_data_dema, bc_col, dema_col)

# Create a summary table
summary_table = pd.DataFrame.from_dict(summary_metrics, orient='index')
summary_table.columns = [
    'Negative values (original)',
    'Negative values (processed)', 
    'Negative reduction',
    'Noise (original ng/m³)', 
    'Noise (processed ng/m³)',
    'Noise reduction factor',
    'Correlation with raw'
]

# Display the summary table
print("\n===== Summary of Method Performance =====")
display(summary_table)

# Compare Source Apportionment Results
print("\n===== Source Apportionment Comparison =====")
print("\nComparison of BB% from Raw, ONA, CMA and DEMA processing:")
print(f"Raw data - Mean BB%: {processed_data_raw_sa['BB_Percent'].mean():.2f}, Std: {processed_data_raw_sa['BB_Percent'].std():.2f}")

if 'BB_Percent' in processed_data_ona_sa.columns:
    print(f"ONA data - Mean BB%: {processed_data_ona_sa['BB_Percent'].mean():.2f}, Std: {processed_data_ona_sa['BB_Percent'].std():.2f}")

print(f"CMA data - Mean BB%: {processed_data_cma_sa['BB_Percent'].mean():.2f}, Std: {processed_data_cma_sa['BB_Percent'].std():.2f}")
print(f"DEMA data - Mean BB%: {processed_data_dema_sa['BB_Percent'].mean():.2f}, Std: {processed_data_dema_sa['BB_Percent'].std():.2f}")

# %% [markdown]
# ## 10. Visualize Results
#   
# Let's visualize the raw and processed data to see the effects of our algorithms.

# %%
def plot_comparison(data, data_ona, data_cma, data_dema, wavelength, sample_period=None):
    """
    Plot the raw, ONA, CMA, and DEMA processed BC data for comparison
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame with raw data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed data
    wavelength : str
        Which wavelength to plot
    sample_period : tuple, optional
        Start and end indices for a subset of the data to plot
    """
    # Identify columns
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    # Check which methods are available
    methods_available = []
    if bc_col in data.columns:
        methods_available.append(('Raw', data, bc_col, 'k-'))
    if ona_col in data_ona.columns:
        methods_available.append(('ONA', data_ona, ona_col, 'b-'))
    if cma_col in data_cma.columns:
        methods_available.append(('CMA', data_cma, cma_col, 'r-'))
    if dema_col in data_dema.columns:
        methods_available.append(('DEMA', data_dema, dema_col, 'g-'))
    
    # If no methods are available, return early
    if not methods_available:
        print(f"No data available for {wavelength} wavelength comparison")
        return
    
    # Use Polars for the data selection and transformation
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_datas = []
        
        for name, df, col, style in methods_available:
            # Convert to Polars for efficient slicing
            pl_df = pl.from_pandas(df)
            
            # Slice the data
            pl_slice = pl_df.slice(start_idx, end_idx - start_idx)
            
            # Convert back to pandas for plotting
            plot_datas.append((name, pl_slice.to_pandas(), col, style))
    else:
        plot_datas = methods_available
    
    # Create a figure
    plt.figure(figsize=(12, 6))
    
    # Create x-axis values - use first dataset for reference
    if len(plot_datas) > 0 and 'Time (UTC)' in plot_datas[0][1].columns:
        try:
            # Convert time column to datetime using pandas
            x = pd.to_datetime(plot_datas[0][1]['Time (UTC)'])
            x_formatter = mdates.DateFormatter('%H:%M')
            plt.gca().xaxis.set_major_formatter(x_formatter)
            plt.gcf().autofmt_xdate()
            x_label = 'Time (UTC)'
        except:
            # If datetime conversion fails, use index
            x = np.arange(len(plot_datas[0][1]))
            x_label = 'Data Point'
    else:
        # If no time column, use index
        x = np.arange(len(plot_datas[0][1]) if len(plot_datas) > 0 else 0)
        x_label = 'Data Point'
    
    # Plot BC data for each method
    for name, df, col, style in plot_datas:
        if name == 'Raw':
            plt.plot(x, df[col], style, alpha=0.5, label=name)
        else:
            plt.plot(x, df[col], style, label=name)
    
    # Set labels and title
    plt.xlabel(x_label)
    plt.ylabel(f'{wavelength} BC (ng/m³)')
    plt.title(f'Comparison of Noise Reduction Methods for {wavelength} Wavelength')
    
    # Add legend and grid
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Improve layout
    plt.tight_layout()
    
    # Get y-limits based on data to ensure consistent scaling
    if len(plot_datas) > 0:
        all_values = []
        for _, df, col, _ in plot_datas:
            all_values.extend(df[col].dropna().tolist())
        
        if all_values:
            # Use percentiles to avoid extreme outliers
            ymin = np.percentile(all_values, 1)  # 1st percentile
            ymax = np.percentile(all_values, 99)  # 99th percentile
            
            # Add a small buffer
            y_range = ymax - ymin
            plt.ylim([ymin - 0.05 * y_range, ymax + 0.05 * y_range])
    
    # Show the plot
    plt.show()
    
    # Optional: Calculate and display statistics for each method
    if len(plot_datas) > 0:
        # Use Polars for efficient stats calculation
        stats_data = []
        for name, df, col, _ in plot_datas:
            pl_df = pl.from_pandas(df[[col]])
            stats = pl_df.select([
                pl.lit(name).alias("Method"),
                pl.col(col).mean().alias("Mean"),
                pl.col(col).median().alias("Median"),
                pl.col(col).min().alias("Min"),
                pl.col(col).max().alias("Max"),
                pl.col(col).std().alias("Std Dev")
            ])
            stats_data.append(stats)
        
        if stats_data:
            # Combine all stats
            combined_stats = pl.concat(stats_data)
            
            # Display the statistics
            print(f"\nStatistics for {wavelength} wavelength:")
            display(combined_stats)

# %%
def plot_source_apportionment_comparison(data_raw, data_ona, data_cma, data_dema, sample_period=None):
    """
    Plot the source apportionment results for raw, ONA, CMA and DEMA data
    
    Parameters:
    -----------
    data_raw : pandas.DataFrame
        DataFrame with raw source apportionment data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed source apportionment data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed source apportionment data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed source apportionment data
    sample_period : tuple, optional
        Start and end indices for a subset of the data to plot
    """
    # Check which methods have source apportionment data
    methods_available = []
    if 'BB_Percent' in data_raw.columns:
        methods_available.append(('Raw', data_raw, 'BB_Percent', 'k-'))
    if 'BB_Percent' in data_ona.columns:
        methods_available.append(('ONA', data_ona, 'BB_Percent', 'b-'))
    if 'BB_Percent' in data_cma.columns:
        methods_available.append(('CMA', data_cma, 'BB_Percent', 'r-'))
    if 'BB_Percent' in data_dema.columns:
        methods_available.append(('DEMA', data_dema, 'BB_Percent', 'g-'))
    
    # If no methods available, return early
    if not methods_available:
        print("No source apportionment data available for comparison")
        return
    
    # Use Polars for efficient data selection if sample period is specified
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_datas = []
        
        for name, df, col, style in methods_available:
            # Convert to Polars for efficient slicing
            pl_df = pl.from_pandas(df)
            
            # Slice the data
            pl_slice = pl_df.slice(start_idx, end_idx - start_idx)
            
            # Convert back to pandas for plotting
            plot_datas.append((name, pl_slice.to_pandas(), col, style))
    else:
        plot_datas = methods_available
    
    # Create a figure with three subplots
    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
    
    # Create x-axis values - use first dataset for reference
    if len(plot_datas) > 0 and 'Time (UTC)' in plot_datas[0][1].columns:
        try:
            # Convert time column to datetime using pandas
            x = pd.to_datetime(plot_datas[0][1]['Time (UTC)'])
            x_formatter = mdates.DateFormatter('%H:%M')
            x_label = 'Time (UTC)'
            for ax in axes:
                ax.xaxis.set_major_formatter(x_formatter)
            fig.autofmt_xdate()
        except:
            # If datetime conversion fails, use index
            x = np.arange(len(plot_datas[0][1]))
            x_label = 'Data Point'
    else:
        # If no time column, use index
        x = np.arange(len(plot_datas[0][1]) if len(plot_datas) > 0 else 0)
        x_label = 'Data Point'
    
    # Get y-limits for biomass burning percentage
    bb_values = []
    for name, df, col, _ in plot_datas:
        bb_values.extend(df[col].dropna().tolist())
    
    if bb_values:
        # Use percentiles to avoid extreme outliers but ensure range is within 0-100%
        bb_min = max(0, np.percentile(bb_values, 1))  # 1st percentile
        bb_max = min(100, np.percentile(bb_values, 99))  # 99th percentile
        
        # Add a small buffer
        bb_range = bb_max - bb_min
        bb_ylim = [max(0, bb_min - 0.05 * bb_range), min(100, bb_max + 0.05 * bb_range)]
        
        # Set y-limits for BB percentage plot
        axes[0].set_ylim(bb_ylim)
    
    # Plot BB percentage for each method
    for name, df, col, style in plot_datas:
        if name == 'Raw':
            axes[0].plot(x, df[col], style, alpha=0.5, label=name)
        else:
            axes[0].plot(x, df[col], style, label=name)
    
    axes[0].set_ylabel('Biomass Burning %')
    axes[0].set_title('Source Apportionment Results Comparison')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # Get wood burning BC values for y-limits
    wb_values = []
    for name, df, _, _ in plot_datas:
        if 'BC_WB' in df.columns:
            wb_values.extend(df['BC_WB'].dropna().tolist())
    
    if wb_values:
        # Set y-limits for wood burning BC plot
        wb_min = np.percentile(wb_values, 1)  # 1st percentile
        wb_max = np.percentile(wb_values, 99)  # 99th percentile
        
        # Add a small buffer
        wb_range = wb_max - wb_min
        axes[1].set_ylim([max(0, wb_min - 0.05 * wb_range), wb_max + 0.05 * wb_range])
    
    # Plot Wood Burning BC for each method
    for name, df, _, style in plot_datas:
        if 'BC_WB' in df.columns:
            if name == 'Raw':
                axes[1].plot(x, df['BC_WB'], style, alpha=0.5, label=name)
            else:
                axes[1].plot(x, df['BC_WB'], style, label=name)
    
    axes[1].set_ylabel('Wood Burning BC (ng/m³)')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    # Get fossil fuel BC values for y-limits
    ff_values = []
    for name, df, _, _ in plot_datas:
        if 'BC_FF' in df.columns:
            ff_values.extend(df['BC_FF'].dropna().tolist())
    
    if ff_values:
        # Set y-limits for fossil fuel BC plot
        ff_min = np.percentile(ff_values, 1)  # 1st percentile
        ff_max = np.percentile(ff_values, 99)  # 99th percentile
        
        # Add a small buffer
        ff_range = ff_max - ff_min
        axes[2].set_ylim([max(0, ff_min - 0.05 * ff_range), ff_max + 0.05 * ff_range])
    
    # Plot Fossil Fuel BC for each method
    for name, df, _, style in plot_datas:
        if 'BC_FF' in df.columns:
            if name == 'Raw':
                axes[2].plot(x, df['BC_FF'], style, alpha=0.5, label=name)
            else:
                axes[2].plot(x, df['BC_FF'], style, label=name)
    
    axes[2].set_ylabel('Fossil Fuel BC (ng/m³)')
    axes[2].set_xlabel(x_label)
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    
    # Adjust layout
    plt.tight_layout()
    
    # Show plot
    plt.show()
    
    # Optional: Calculate and display statistics for each source apportionment component
    if len(plot_datas) > 0:
        # Use Polars for efficient stats calculation
        components = ['BB_Percent', 'BC_WB', 'BC_FF']
        stats_data = []
        
        for name, df, _, _ in plot_datas:
            for component in components:
                if component in df.columns:
                    pl_df = pl.from_pandas(df[[component]])
                    stats = pl_df.select([
                        pl.lit(name).alias("Method"),
                        pl.lit(component).alias("Component"),
                        pl.col(component).mean().alias("Mean"),
                        pl.col(component).median().alias("Median"),
                        pl.col(component).min().alias("Min"),
                        pl.col(component).max().alias("Max"),
                        pl.col(component).std().alias("Std Dev")
                    ])
                    stats_data.append(stats)
        
        if stats_data:
            # Combine all stats
            combined_stats = pl.concat(stats_data)
            
            # Display the statistics
            print("\nSource Apportionment Statistics:")
            display(combined_stats)

# %% [markdown]
# Let's plot the comparison of all methods for each wavelength

# %%
# Plot comparison for each wavelength
for wavelength in ['Blue', 'IR']:
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    if bc_col in data.columns:
        print(f"\nPlots for {wavelength} wavelength comparison:")
        
        # Plot full dataset
        plot_comparison(data, processed_data_ona, processed_data_cma, processed_data_dema, wavelength)
        
        # Plot a sample period (first 1000 points or 10% of data, whichever is smaller)
        sample_size = min(1000, int(len(data) * 0.1))
        if sample_size < len(data):
            print(f"\nZoomed view of first {sample_size} points:")
            plot_comparison(data, processed_data_ona, processed_data_cma, processed_data_dema, wavelength, (0, sample_size))

# Plot source apportionment comparisons
print("\nSource apportionment comparison plots:")
plot_source_apportionment_comparison(processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa, processed_data_dema_sa)

# Plot a sample period
sample_size = min(1000, int(len(data) * 0.1))
if sample_size < len(data):
    print(f"\nZoomed view of first {sample_size} points:")
    plot_source_apportionment_comparison(processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa, processed_data_dema_sa, (0, sample_size))


# %% [markdown]
# ## 11. Side-by-Side Visualization Comparison
#   
# Now let's create a side-by-side comparison of all methods for easier visual comparison.

# %%
def plot_side_by_side_comparison(data, data_ona, data_cma, data_dema, wavelength='Blue', sample_period=None, timebase=60):
    """
    Plot raw, ONA, CMA, and DEMA processed BC data in a side-by-side grid layout.
    """

    # ---------- identify columns ------------------------------------------------
    bc_col   = f"{wavelength} BCc"
    ona_col  = f"{wavelength}_BC_ONA"
    cma_col  = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"

    # ---------- assemble available methods --------------------------------------
    methods = []
    if bc_col  in data.columns:       methods.append(("Raw",  data,       bc_col))
    if ona_col in data_ona.columns:   methods.append(("ONA",  data_ona,   ona_col))
    if cma_col in data_cma.columns:   methods.append(("CMA",  data_cma,   cma_col))
    if dema_col in data_dema.columns: methods.append(("DEMA", data_dema,  dema_col))

    # ---------- optionally slice a sample period --------------------------------
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_methods = []
        for name, df, col in methods:
            pl_slice = pl.from_pandas(df).slice(start_idx, end_idx - start_idx)
            plot_methods.append((name, pl_slice.to_pandas(), col))
    else:
        plot_methods = methods

    # ---------- guard clause ----------------------------------------------------
    n_methods = len(plot_methods)
    if n_methods < 2:
        print("Not enough methods to compare")
        return

    # ---------- figure layout ---------------------------------------------------
    total_panels = n_methods + 1          # +1 for the comparison panel
    n_rows = int(np.ceil(np.sqrt(total_panels)))
    n_cols = int(np.ceil(total_panels / n_rows))
    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(n_cols * 5, n_rows * 4),
        sharex=True, sharey=True
    )
    axes = np.array(axes).flatten()

    # ---------- x-axis ----------------------------------------------------------
    if "Time (UTC)" in plot_methods[0][1].columns:
        try:
            x = pd.to_datetime(plot_methods[0][1]["Time (UTC)"])
            x_label = "Time (UTC)"
            formatter = mdates.DateFormatter("%H:%M")
            for ax in axes:
                ax.xaxis.set_major_formatter(formatter)
            fig.autofmt_xdate()
        except Exception:
            x = np.arange(len(plot_methods[0][1]))
            x_label = "Data Point"
    else:
        x = np.arange(len(plot_methods[0][1]))
        x_label = "Data Point"

    # ---------- y-limits (fixed) ------------------------------------------------
    all_y_values = []
    for _, plot_data, col in plot_methods:
        pl_data = pl.from_pandas(plot_data[[col]])
        valid_vals = (
            pl_data
            .filter(~pl.col(col).is_null())
            .select(pl.col(col).alias("value"))            # <<< rename here
        )
        if valid_vals.height > 0:
            all_y_values.append(valid_vals)

    if all_y_values:
        combined = pl.concat(all_y_values)                 # now columns match
        p01, p99 = combined.select(
            pl.col("value").quantile(0.01).alias("p01"),
            pl.col("value").quantile(0.99).alias("p99")
        ).row(0)
        y_buffer = (p99 - p01) * 0.10
        ylim = (p01 - y_buffer, p99 + y_buffer)
    else:
        ylim = (0, 1)

    # ---------- individual panels ----------------------------------------------
    for i, (name, plot_data, col) in enumerate(plot_methods):
        title = "Raw data" if name == "Raw" else f"{name} processed"
        title += f" ({timebase}s)"
        colour = {"Raw": "k", "ONA": "b", "CMA": "r", "DEMA": "g"}[name]
        axes[i].plot(x, plot_data[col], f"{colour}-", label=name, alpha=0.8)
        axes[i].set_title(title)
        axes[i].set_ylabel(f"{wavelength} BC (ng/m³)")
        axes[i].set_ylim(ylim)
        axes[i].grid(True, alpha=0.3)

    # ---------- comparison panel ------------------------------------------------
    comp_idx = n_methods
    for name, plot_data, col in plot_methods:
        colour = {"Raw": "k", "ONA": "b", "CMA": "r", "DEMA": "g"}[name]
        alpha  = 0.5 if name == "Raw" else 0.8
        axes[comp_idx].plot(x, plot_data[col], f"{colour}-", label=name, alpha=alpha)

    axes[comp_idx].set_title("All methods comparison")
    axes[comp_idx].set_xlabel(x_label)
    axes[comp_idx].set_ylabel(f"{wavelength} BC (ng/m³)")
    axes[comp_idx].set_ylim(ylim)
    axes[comp_idx].grid(True, alpha=0.3)
    axes[comp_idx].legend()

    # ---------- tidy up ---------------------------------------------------------
    for j in range(total_panels, len(axes)):
        axes[j].set_visible(False)

    plt.tight_layout()
    plt.suptitle(
        f"Side-by-Side Comparison of Processing Methods ({wavelength} Wavelength)",
        y=1.02,
        fontsize=16
    )
    plt.figtext(
        0.5,
        -0.01,
        "Methods: Raw = Original; ONA = Optimized Noise Algorithm; "
        "CMA = Centered Moving Average; DEMA = Double Exponential Moving Average",
        ha="center",
        fontsize=10,
        wrap=True
    )
    plt.show()

    # ---------- optional: return summary stats ----------------------------------
    stats_frames = []
    for name, plot_data, col in plot_methods:
        stats_frames.append(
            pl.from_pandas(plot_data[[col]]).select(
                pl.lit(name).alias("Method"),
                pl.lit(wavelength).alias("Wavelength"),
                pl.col(col).mean().alias("Mean"),
                pl.col(col).median().alias("Median"),
                pl.col(col).min().alias("Min"),
                pl.col(col).max().alias("Max"),
                pl.col(col).std().alias("Std Dev"),
            )
        )
    return pl.concat(stats_frames) if stats_frames else None

# %%
def plot_source_apportionment_side_by_side(data_raw, data_ona, data_cma, data_dema, sample_period=None, timebase=60):
    """
    Plot source apportionment results in a side-by-side grid layout
    
    Parameters:
    -----------
    data_raw : pandas.DataFrame
        DataFrame with raw source apportionment data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed source apportionment data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed source apportionment data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed source apportionment data
    sample_period : tuple, optional
        Start and end indices for a subset of the data to plot
    timebase : int
        The timebase in seconds (5, 10, 30, 60, etc.)
    """
    # Determine which methods have source apportionment data
    methods = []
    if 'BB_Percent' in data_raw.columns:
        methods.append(('Raw', data_raw))
    if 'BB_Percent' in data_ona.columns:
        methods.append(('ONA', data_ona))
    if 'BB_Percent' in data_cma.columns:
        methods.append(('CMA', data_cma))
    if 'BB_Percent' in data_dema.columns:
        methods.append(('DEMA', data_dema))
    
    # Use Polars for efficient data selection if sample period is specified
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_methods = []
        
        for name, df in methods:
            # Convert to Polars for efficient slicing
            pl_df = pl.from_pandas(df)
            
            # Slice the data
            pl_slice = pl_df.slice(start_idx, end_idx - start_idx)
            
            # Convert back to pandas for plotting
            plot_methods.append((name, pl_slice.to_pandas()))
    else:
        plot_methods = methods
    
    # Determine number of panels needed (number of methods + 1 for comparison)
    n_methods = len(plot_methods)
    if n_methods < 2:
        print("Not enough methods to compare")
        return
    
    total_panels = n_methods + 1  # Include comparison panel
    
    # Calculate grid dimensions (approximately square)
    n_rows = int(np.ceil(np.sqrt(total_panels)))
    n_cols = int(np.ceil(total_panels / n_rows))
    
    # Create a figure with a grid of subplots
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols*5, n_rows*4), sharex=True, sharey=True)
    
    # Handle the case of a single row or column
    if n_rows == 1 and n_cols == 1:
        axes = np.array([axes]) 
    
    # Flatten for easier indexing
    axes = axes.flatten()
    
    # Create x-axis values - use first dataset for reference
    if 'Time (UTC)' in plot_methods[0][1].columns:
        try:
            # Convert time column to datetime using pandas
            x = pd.to_datetime(plot_methods[0][1]['Time (UTC)'])
            x_formatter = mdates.DateFormatter('%H:%M')
            x_label = 'Time (UTC)'
            for ax in axes:
                ax.xaxis.set_major_formatter(x_formatter)
            fig.autofmt_xdate()
        except:
            # If datetime conversion fails, use index
            x = np.arange(len(plot_methods[0][1]))
            x_label = 'Data Point'
    else:
        # If no time column, use index
        x = np.arange(len(plot_methods[0][1]))
        x_label = 'Data Point'
    
    # Use Polars for efficient calculation of y-limits for BB percentage
    all_bb_values = []
    for name, plot_data in plot_methods:
        # Convert to Polars for more efficient filtering
        pl_data = pl.from_pandas(plot_data[['BB_Percent']])
        
        # Get valid values (non-null)
        valid_values = pl_data.filter(~pl.col('BB_Percent').is_null())
        
        # Add to the list if there are valid values
        if valid_values.height > 0:
            all_bb_values.append(valid_values.select(pl.col('BB_Percent')))
    
    if all_bb_values:
        # Concatenate all valid values
        combined_values = pl.concat(all_bb_values)
        
        # Calculate percentiles with clipping to 0-100 range
        # MODIFIED SECTION
        percentiles = combined_values.select([
            pl.max_horizontal([pl.col('BB_Percent').quantile(0.01), pl.lit(0)]).alias("p01"),
            pl.min_horizontal([pl.col('BB_Percent').quantile(0.99), pl.lit(100)]).alias("p99")
        ])
        # END MODIFIED SECTION
        
        ymin_bb = percentiles[0, 0]  # 1st percentile, but not below 0
        ymax_bb = percentiles[0, 1]  # 99th percentile, but not above 100
        
        # Add a small buffer to the y-limits for visual clarity
        y_buffer_bb = (ymax_bb - ymin_bb) * 0.1
        ylim_bb = (max(0, ymin_bb - y_buffer_bb), min(100, ymax_bb + y_buffer_bb))
    else:
        ylim_bb = (0, 100)  # Default range for BB%
    
    # Plot titles and data for individual methods
    for i, (name, plot_data) in enumerate(plot_methods):
        title = f'{name} processed ({timebase}s)'
        if name == 'Raw':
            title = f'Raw data ({timebase}s)'
            axes[i].plot(x, plot_data['BB_Percent'], 'k-', label=name)
        elif name == 'ONA':
            axes[i].plot(x, plot_data['BB_Percent'], 'b-', label=name)
        elif name == 'CMA':
            axes[i].plot(x, plot_data['BB_Percent'], 'r-', label=name)
        elif name == 'DEMA':
            axes[i].plot(x, plot_data['BB_Percent'], 'g-', label=name)
        
        axes[i].set_title(title)
        axes[i].set_ylabel('Biomass Burning %')
        axes[i].set_ylim(ylim_bb)
        axes[i].grid(True, alpha=0.3)
    
    # Use the comparison panel (should be the next one after all individual method panels)
    comp_idx = n_methods  # This should be the index for the comparison panel
    
    # Plot all methods for comparison in the comparison panel
    for name, plot_data in plot_methods:
        if name == 'Raw':
            axes[comp_idx].plot(x, plot_data['BB_Percent'], 'k-', alpha=0.5, label=name)
        elif name == 'ONA':
            axes[comp_idx].plot(x, plot_data['BB_Percent'], 'b-', label=name)
        elif name == 'CMA':
            axes[comp_idx].plot(x, plot_data['BB_Percent'], 'r-', label=name)
        elif name == 'DEMA':
            axes[comp_idx].plot(x, plot_data['BB_Percent'], 'g-', label=name)
    
    axes[comp_idx].set_title('All methods comparison')
    axes[comp_idx].set_ylabel('Biomass Burning %')
    axes[comp_idx].set_xlabel(x_label)
    axes[comp_idx].set_ylim(ylim_bb)
    axes[comp_idx].grid(True, alpha=0.3)
    axes[comp_idx].legend()
    
    # Hide any extra subplots
    for i in range(total_panels, len(axes)):
        axes[i].set_visible(False)
    
    if x_label == 'Time (UTC)':
        fig.autofmt_xdate()
    
    plt.tight_layout()
    plt.suptitle('Side-by-Side Comparison of Source Apportionment Results', y=1.02, fontsize=16)
    
    # Add method information in the figure footer
    plt.figtext(0.5, -0.01, 
                "Biomass Burning (BB) % shows the proportion of black carbon from wood/biomass combustion vs. fossil fuels", 
                ha='center', fontsize=10, wrap=True)
    
    plt.show()
    
    # Optional: Return statistics about source apportionment
    stats_data = []
    for name, plot_data in plot_methods:
        # Use Polars for efficient stats calculation
        pl_data = pl.from_pandas(plot_data[['BB_Percent']])
        stats = pl_data.select([
            pl.lit(name).alias("Method"),
            pl.col('BB_Percent').mean().alias("Mean BB%"),
            pl.col('BB_Percent').median().alias("Median BB%"),
            pl.col('BB_Percent').min().alias("Min BB%"),
            pl.col('BB_Percent').max().alias("Max BB%"),
            pl.col('BB_Percent').std().alias("Std Dev BB%")
        ])
        stats_data.append(stats)
    
    # Return statistics if we have data
    if stats_data:
        return pl.concat(stats_data)
    
    return None

# %%
# Determine the timebase from the data using Polars
pl_data = pl.from_pandas(data)
if 'Timebase (s)' in pl_data.columns:
    timebase = pl_data.select(pl.col('Timebase (s)')).row(0)[0]
else:
    timebase = 60  # default assumption

# Plot side-by-side comparison for each wavelength
for wavelength in ['Blue', 'IR']:
    bc_col = f"{wavelength} BCc"
    
    if bc_col in pl_data.columns:
        print(f"\nSide-by-side comparison for {wavelength} wavelength:")
        
        # Plot full dataset
        plot_side_by_side_comparison(data, processed_data_ona, processed_data_cma, processed_data_dema,
                                  wavelength, timebase=timebase)
        
        # Calculate sample size using Polars for efficiency
        data_length = pl_data.height
        sample_size = min(1000, int(data_length * 0.1))
        
        if sample_size < data_length:
            print(f"\nZoomed view of first {sample_size} points:")
            plot_side_by_side_comparison(data, processed_data_ona, processed_data_cma, processed_data_dema,
                                      wavelength, (0, sample_size), timebase=timebase)

# Plot source apportionment comparisons
print("\nSide-by-side source apportionment comparison:")
sa_stats = plot_source_apportionment_side_by_side(processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa,
                                     processed_data_dema_sa, timebase=timebase)

# If statistics were returned, display them
if sa_stats is not None:
    print("\nSource Apportionment Statistics:")
    display(sa_stats)

# Plot a sample period for source apportionment using the same sample size for consistency
if sample_size < data_length:
    print(f"\nZoomed view of first {sample_size} points for source apportionment:")
    sa_zoom_stats = plot_source_apportionment_side_by_side(processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa,
                                        processed_data_dema_sa, (0, sample_size), timebase=timebase)
    
    # If zoom statistics were returned, display them
    if sa_zoom_stats is not None:
        print("\nZoomed Source Apportionment Statistics:")
        display(sa_zoom_stats)

# %% [markdown]
# ## 12. Stacked Temporal Comparison
#   
# Let's also create a visualization that stacks the temporal plots vertically for each processing method.

# %%
def plot_stacked_temporal_comparison(data, data_ona, data_cma, data_dema, wavelength='Blue', sample_period=None):
    """
    Plot stacked temporal comparison of raw, ONA, CMA, and DEMA processed BC data
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame with raw data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed data
    wavelength : str
        Which wavelength to plot
    sample_period : tuple, optional
        Start and end indices for a subset of the data to plot
    """
    # Identify columns
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    # Determine which methods are available
    methods = []
    if bc_col in data.columns:
        methods.append(('Raw', data, bc_col))
    if ona_col in data_ona.columns:
        methods.append(('ONA', data_ona, ona_col))
    if cma_col in data_cma.columns:
        methods.append(('CMA', data_cma, cma_col))
    if dema_col in data_dema.columns:
        methods.append(('DEMA', data_dema, dema_col))
    
    # Use Polars for efficient data selection if sample period is specified
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_methods = []
        
        for name, df, col in methods:
            # Convert to Polars for efficient slicing
            pl_df = pl.from_pandas(df)
            
            # Slice the data
            pl_slice = pl_df.slice(start_idx, end_idx - start_idx)
            
            # Convert back to pandas for plotting
            plot_methods.append((name, pl_slice.to_pandas(), col))
    else:
        plot_methods = methods
    
    # Determine number of panels needed
    n_methods = len(plot_methods)
    if n_methods < 2:
        print("Not enough methods to compare")
        return
    
    # Create a figure with vertically stacked subplots
    fig, axes = plt.subplots(n_methods, 1, figsize=(12, n_methods*3), sharex=True)
    if n_methods == 1:
        axes = [axes]  # Make it a list for consistent indexing
    
    # Determine the timebase using Polars for efficiency
    if 'Timebase (s)' in plot_methods[0][1].columns:
        # Convert to Polars for efficient access
        pl_first_data = pl.from_pandas(plot_methods[0][1][['Timebase (s)']])
        timebase = pl_first_data.select(pl.col('Timebase (s)')).row(0)[0]
    else:
        timebase = 60  # default
    
    # Create x-axis values - use first dataset for reference
    if 'Time (UTC)' in plot_methods[0][1].columns:
        try:
            # Convert time column to datetime using pandas
            x = pd.to_datetime(plot_methods[0][1]['Time (UTC)'])
            x_formatter = mdates.DateFormatter('%H:%M')
            x_label = 'Time (UTC)'
            for ax in axes:
                ax.xaxis.set_major_formatter(x_formatter)
            fig.autofmt_xdate()
        except:
            # If datetime conversion fails, use index
            x = np.arange(len(plot_methods[0][1]))
            x_label = 'Data Point'
    else:
        # If no time column, use index
        x = np.arange(len(plot_methods[0][1]))
        x_label = 'Data Point'
    
    # Use Polars for efficient calculation of y-limits
    all_y_values = []
    for name, plot_data, col in plot_methods:
        # Convert to Polars for more efficient filtering
        pl_df = pl.from_pandas(plot_data[[col]]) # Corrected from pl_data to pl_df
        
        # Get valid values (non-null)
        valid_values = pl_df.filter(~pl.col(col).is_null()) # Corrected from pl_data to pl_df
        
        # Add to the list if there are valid values
        if valid_values.height > 0:
            # >>> Corrected line: Alias the column to a common name "value"
            all_y_values.append(valid_values.select(pl.col(col).alias("value")))
    
    if all_y_values:
        # Concatenate all valid values
        combined_values = pl.concat(all_y_values)
        
        # Calculate percentiles
        # >>> Corrected lines: Use the aliased column name "value"
        percentiles = combined_values.select([
            pl.col("value").quantile(0.01).alias("p01"),
            pl.col("value").quantile(0.99).alias("p99")
        ])
        
        ymin = percentiles[0, "p01"]  # Access by alias
        ymax = percentiles[0, "p99"]  # Access by alias
        
        # Ensure min and max are different to prevent division by zero
        if ymin == ymax:
            ymin -= 10
            ymax += 10
            
        # Add a small buffer to the y-limits for visual clarity
        y_buffer = (ymax - ymin) * 0.1
        ylim = (ymin - y_buffer, ymax + y_buffer)
    else:
        ylim = (0, 1)  # Default if no valid values
    
    # Plot data for each method
    for i, (name, plot_data, col) in enumerate(plot_methods):
        if name == 'Raw':
            axes[i].plot(x, plot_data[col], 'k-', label=f'Raw ({timebase}s)')
        elif name == 'ONA':
            axes[i].plot(x, plot_data[col], 'b-', label=f'ONA ({timebase}s)')
        elif name == 'CMA':
            axes[i].plot(x, plot_data[col], 'r-', label=f'CMA ({timebase}s)')
        elif name == 'DEMA':
            axes[i].plot(x, plot_data[col], 'g-', label=f'DEMA ({timebase}s)')
        
        axes[i].set_ylabel(f'{wavelength} BC (ng/m³)')
        axes[i].legend(loc='upper right')
        axes[i].grid(True, alpha=0.3)
        axes[i].set_ylim(ylim)
    
    axes[-1].set_xlabel(x_label)
    plt.suptitle(f'Stacked Temporal Comparison ({wavelength} Wavelength)', fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(top=0.95)  # Adjust for suptitle
    
    # Add descriptive text about the methods
    method_descriptions = {
        'Raw': 'Unprocessed data',
        'ONA': 'Optimized Noise-reduction Algorithm (adaptive time-averaging based on ΔATN)',
        'CMA': 'Centered Moving Average (fixed window smoothing)',
        'DEMA': 'Double Exponentially Weighted Moving Average (reduces noise with minimal lag)'
    }
    
    available_method_names = [name for name, _, _ in plot_methods]
    description_text = " | ".join([f"{name}: {method_descriptions[name]}" for name in available_method_names])
    
    plt.figtext(0.5, -0.01, description_text, ha='center', fontsize=9, wrap=True)
    
    plt.show()
    
    # Optional: Return statistics for each method
    stats_data = []
    for name, plot_data, col in plot_methods:
        # Use Polars for efficient stats calculation
        pl_df_stats = pl.from_pandas(plot_data[[col]]) # Corrected from pl_data to pl_df_stats
        stats = pl_df_stats.select([ # Corrected from pl_data to pl_df_stats
            pl.lit(name).alias("Method"),
            pl.lit(wavelength).alias("Wavelength"),
            pl.col(col).mean().alias("Mean"),
            pl.col(col).median().alias("Median"),
            pl.col(col).min().alias("Min"),
            pl.col(col).max().alias("Max"),
            pl.col(col).std().alias("Std Dev")
        ])
        stats_data.append(stats)
    
    # Return statistics if we have data
    if stats_data:
        return pl.concat(stats_data)
    
    return None

# %%
def plot_stacked_source_apportionment(data_raw, data_ona, data_cma, data_dema, sample_period=None):
    """
    Plot stacked temporal comparison of source apportionment results
    
    Parameters:
    -----------
    data_raw : pandas.DataFrame
        DataFrame with raw source apportionment data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed source apportionment data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed source apportionment data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed source apportionment data
    sample_period : tuple, optional
        Start and end indices for a subset of the data to plot
    """
    # Determine which methods have source apportionment data
    methods = []
    if 'BB_Percent' in data_raw.columns:
        methods.append(('Raw', data_raw))
    if 'BB_Percent' in data_ona.columns:
        methods.append(('ONA', data_ona))
    if 'BB_Percent' in data_cma.columns:
        methods.append(('CMA', data_cma))
    if 'BB_Percent' in data_dema.columns:
        methods.append(('DEMA', data_dema))
    
    # Use Polars for efficient data selection if sample period is specified
    if sample_period is not None:
        start_idx, end_idx = sample_period
        plot_methods = []
        
        for name, df in methods:
            # Convert to Polars for efficient slicing
            pl_df = pl.from_pandas(df)
            
            # Slice the data
            pl_slice = pl_df.slice(start_idx, end_idx - start_idx)
            
            # Convert back to pandas for plotting
            plot_methods.append((name, pl_slice.to_pandas()))
    else:
        plot_methods = methods
    
    # Determine number of panels needed
    n_methods = len(plot_methods)
    if n_methods < 2:
        print("Not enough methods to compare")
        return
    
    # Create a figure with vertically stacked subplots
    fig, axes = plt.subplots(n_methods, 1, figsize=(12, n_methods*3), sharex=True)
    if n_methods == 1:
        axes = [axes]  # Make it a list for consistent indexing
    
    # Determine the timebase using Polars for efficiency
    if 'Timebase (s)' in plot_methods[0][1].columns:
        # Convert to Polars for efficient access
        pl_first_data = pl.from_pandas(plot_methods[0][1][['Timebase (s)']])
        timebase = pl_first_data.select(pl.col('Timebase (s)')).row(0)[0]
    else:
        timebase = 60  # default
    
    # Create x-axis values - use first dataset for reference
    if 'Time (UTC)' in plot_methods[0][1].columns:
        try:
            # Convert time column to datetime using pandas
            x = pd.to_datetime(plot_methods[0][1]['Time (UTC)'])
            x_formatter = mdates.DateFormatter('%H:%M')
            x_label = 'Time (UTC)'
            for ax in axes:
                ax.xaxis.set_major_formatter(x_formatter)
            fig.autofmt_xdate()
        except:
            # If datetime conversion fails, use index
            x = np.arange(len(plot_methods[0][1]))
            x_label = 'Data Point'
    else:
        # If no time column, use index
        x = np.arange(len(plot_methods[0][1]))
        x_label = 'Data Point'
    
    # Use Polars for efficient calculation of y-limits for BB percentage
    all_bb_values = []
    for name, plot_data in plot_methods:
        # Convert to Polars for more efficient filtering
        pl_data = pl.from_pandas(plot_data[['BB_Percent']])
        
        # Get valid values (non-null)
        valid_values = pl_data.filter(~pl.col('BB_Percent').is_null())
        
        # Add to the list if there are valid values
        if valid_values.height > 0:
            all_bb_values.append(valid_values.select(pl.col('BB_Percent')))
    
    if all_bb_values:
        # Concatenate all valid values
        combined_values = pl.concat(all_bb_values)

        # Calculate percentiles with clipping to 0-100 range
        # MODIFIED SECTION: Changed pl.max to pl.max_horizontal and pl.min to pl.min_horizontal
        percentiles = combined_values.select([
            pl.max_horizontal([pl.col('BB_Percent').quantile(0.01), pl.lit(0)]).alias("p01"),
            pl.min_horizontal([pl.col('BB_Percent').quantile(0.99), pl.lit(100)]).alias("p99")
        ])
        # END MODIFIED SECTION

        ymin = percentiles[0, 0]  # 1st percentile, but not below 0
        ymax = percentiles[0, 1]  # 99th percentile, but not above 100
        
        # Ensure min and max are different to prevent division by zero
        if ymin == ymax:
            if ymin == 0:
                ymax = 10  # If all zeros, set max to 10%
            elif ymax == 100:
                ymin = 90  # If all 100s, set min to 90%
            else:
                # Otherwise add/subtract 5 percentage points
                ymin = max(0, ymin - 5)
                ymax = min(100, ymax + 5)
            
        # Add a small buffer to the y-limits for visual clarity
        y_buffer = (ymax - ymin) * 0.1
        ylim = (max(0, ymin - y_buffer), min(100, ymax + y_buffer))
    else:
        ylim = (0, 100)  # Default range for BB%
    
    # Plot data for each method
    for i, (name, plot_data) in enumerate(plot_methods):
        if name == 'Raw':
            axes[i].plot(x, plot_data['BB_Percent'], 'k-', label=f'Raw ({timebase}s)')
        elif name == 'ONA':
            axes[i].plot(x, plot_data['BB_Percent'], 'b-', label=f'ONA ({timebase}s)')
        elif name == 'CMA':
            axes[i].plot(x, plot_data['BB_Percent'], 'r-', label=f'CMA ({timebase}s)')
        elif name == 'DEMA':
            axes[i].plot(x, plot_data['BB_Percent'], 'g-', label=f'DEMA ({timebase}s)')
        
        axes[i].set_ylabel('Biomass Burning %')
        axes[i].legend(loc='upper right')
        axes[i].grid(True, alpha=0.3)
        axes[i].set_ylim(ylim)
    
    axes[-1].set_xlabel(x_label)
    plt.suptitle('Stacked Temporal Comparison of Source Apportionment Results', fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(top=0.95)  # Adjust for suptitle
    
    # Add information about source apportionment in the figure footer
    plt.figtext(0.5, -0.01, 
                "Biomass Burning (BB) % shows the estimated proportion of black carbon from wood/biomass burning versus fossil fuel combustion", 
                ha='center', fontsize=9, wrap=True)
    
    plt.show()
    
    # Optional: Return statistics about source apportionment
    stats_data = []
    for name, plot_data in plot_methods:
        # Use Polars for efficient stats calculation
        pl_data = pl.from_pandas(plot_data[['BB_Percent']])
        stats = pl_data.select([
            pl.lit(name).alias("Method"),
            pl.col('BB_Percent').mean().alias("Mean BB%"),
            pl.col('BB_Percent').median().alias("Median BB%"),
            pl.col('BB_Percent').min().alias("Min BB%"),
            pl.col('BB_Percent').max().alias("Max BB%"),
            pl.col('BB_Percent').std().alias("Std Dev BB%")
        ])
        stats_data.append(stats)
    
    # Return statistics if we have data
    if stats_data:
        return pl.concat(stats_data)
    
    return None

# %%
# Plot stacked temporal comparison for each wavelength
for wavelength in ['Blue', 'IR']:
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    # Use Polars to check if required columns exist
    pl_data = pl.from_pandas(data)
    pl_ona = pl.from_pandas(processed_data_ona)
    pl_cma = pl.from_pandas(processed_data_cma)
    pl_dema = pl.from_pandas(processed_data_dema)
    
    methods_available = []
    if bc_col in pl_data.columns:
        methods_available.append('Raw')
    if ona_col in pl_ona.columns:
        methods_available.append('ONA')
    if cma_col in pl_cma.columns:
        methods_available.append('CMA')
    if dema_col in pl_dema.columns:
        methods_available.append('DEMA')
    
    # Only proceed if at least two methods are available
    if len(methods_available) >= 2:
        print(f"\nStacked temporal comparison for {wavelength} wavelength:")
        
        # Plot full dataset
        tc_stats = plot_stacked_temporal_comparison(data, processed_data_ona, processed_data_cma, processed_data_dema, wavelength)
        
        # Display statistics if returned
        if tc_stats is not None:
            print(f"\nStatistics for {wavelength} wavelength:")
            display(tc_stats)
        
        # Calculate sample size using Polars for efficiency
        data_length = pl_data.height
        sample_size = min(1000, int(data_length * 0.1))
        
        if sample_size < data_length:
            print(f"\nZoomed view of first {sample_size} points:")
            tc_zoom_stats = plot_stacked_temporal_comparison(
                data, processed_data_ona, processed_data_cma, processed_data_dema, 
                wavelength, (0, sample_size)
            )
            
            # Display zoom statistics if returned
            if tc_zoom_stats is not None:
                print(f"\nZoomed Statistics for {wavelength} wavelength:")
                display(tc_zoom_stats)

# Plot stacked source apportionment comparisons
# Use Polars to check if source apportionment data is available for at least two methods
pl_raw_sa = pl.from_pandas(processed_data_raw_sa)
pl_ona_sa = pl.from_pandas(processed_data_ona_sa)
pl_cma_sa = pl.from_pandas(processed_data_cma_sa)
pl_dema_sa = pl.from_pandas(processed_data_dema_sa)

sa_methods_available = []
if 'BB_Percent' in pl_raw_sa.columns:
    sa_methods_available.append('Raw')
if 'BB_Percent' in pl_ona_sa.columns:
    sa_methods_available.append('ONA')
if 'BB_Percent' in pl_cma_sa.columns:
    sa_methods_available.append('CMA')
if 'BB_Percent' in pl_dema_sa.columns:
    sa_methods_available.append('DEMA')

if len(sa_methods_available) >= 2:
    print("\nStacked source apportionment comparison:")
    
    # Plot full source apportionment dataset
    sa_stats = plot_stacked_source_apportionment(
        processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa, processed_data_dema_sa
    )
    
    # Display statistics if returned
    if sa_stats is not None:
        print("\nSource Apportionment Statistics:")
        display(sa_stats)

    # Calculate sample size using Polars for efficiency
    # Reuse data_length from previous calculation
    if 'data_length' not in locals():
        pl_data = pl.from_pandas(data)
        data_length = pl_data.height
        
    sample_size = min(1000, int(data_length * 0.1))
    
    if sample_size < data_length:
        print(f"\nZoomed view of first {sample_size} points for source apportionment:")
        
        # Plot zoomed source apportionment
        sa_zoom_stats = plot_stacked_source_apportionment(
            processed_data_raw_sa, processed_data_ona_sa, processed_data_cma_sa,
            processed_data_dema_sa, (0, sample_size)
        )
        
        # Display zoom statistics if returned
        if sa_zoom_stats is not None:
            print("\nZoomed Source Apportionment Statistics:")
            display(sa_zoom_stats)

# %% [markdown]
# ## 13. Time Averaging Analysis
#  
# Let's analyze the time averaging behavior of the ONA method and compare it to the effective averaging window of the CMA and DEMA methods.

# %%
@numba.jit(nopython=True)
def _numba_nanmean(arr_slice): # Make sure this is defined if used by ONA points calculation
    finite_sum = 0.0
    finite_count = 0
    for x in arr_slice:
        if not np.isnan(x):
            finite_sum += x
            finite_count += 1
    if finite_count == 0:
        return np.nan
    return finite_sum / finite_count

def analyze_time_averaging(data_ona, data_cma, data_dema, wavelength='Blue'):
    """
    Analyze and compare the time averaging behavior of each method.
    Ensure data_ona, data_cma, data_dema are Pandas DataFrames for this version.
    """
    methods = [] # List to store tuples: (method_name, points_series_for_plotting_pd, stats_dict)
    
    # --- Populate 'methods' list ---
    # This section needs your specific logic for calculating ONA points, CMA window, and DEMA equivalent window.
    # Ensure 'points_series_for_plotting_pd' is a Pandas Series (e.g., for ONA's varying points).
    # Ensure 'stats_dict' contains {"mean": float, "median": float, "min": float, "max": float, "std": float}.

    # Example: ONA (ensure data_ona is a Pandas DataFrame)
    points_averaged_col = f"{wavelength}_points_averaged"
    if isinstance(data_ona, pd.DataFrame) and points_averaged_col in data_ona.columns:
        ona_points_pd = data_ona[points_averaged_col].dropna() 
        if not ona_points_pd.empty:
            ona_stats_dict = {
                "mean": float(ona_points_pd.mean()), "median": float(ona_points_pd.median()),
                "min": float(ona_points_pd.min()), "max": float(ona_points_pd.max()), 
                "std": float(ona_points_pd.std()) if ona_points_pd.std() is not None else np.nan
            }
            # Ensure std is nan if it cannot be computed (e.g. single point)
            if pd.isna(ona_stats_dict["std"]) and len(ona_points_pd) <=1 : ona_stats_dict["std"] = 0.0
            elif pd.isna(ona_stats_dict["std"]): ona_stats_dict["std"] = np.nan

            methods.append(('ONA', ona_points_pd, ona_stats_dict))
        else:
            print(f"Warning: ONA points column '{points_averaged_col}' is empty for wavelength {wavelength}.")
    else:
        print(f"Warning: ONA data or points column '{points_averaged_col}' not found/valid for wavelength {wavelength}.")

    # Example: CMA (ensure data_cma is a Pandas DataFrame)
    if isinstance(data_cma, pd.DataFrame) and f"{wavelength}_BC_CMA" in data_cma.columns:
        timebase = data_cma['Timebase (s)'].iloc[0] if 'Timebase (s)' in data_cma.columns and not data_cma.empty else 60
        cma_window = 5 # Default, apply your logic
        if timebase == 1: cma_window = 11
        elif timebase == 5: cma_window = 5
        elif timebase == 60: cma_window = 3
        if cma_window % 2 == 0: cma_window += 1
        
        cma_points_pd = pd.Series([cma_window] * len(data_cma)) # For consistency in plotting if needed
        cma_stats_dict = {"mean": float(cma_window), "median": float(cma_window), 
                          "min": float(cma_window), "max": float(cma_window), "std": 0.0}
        methods.append(('CMA', cma_points_pd, cma_stats_dict))
    else:
        print(f"Warning: CMA data or BC column not found/valid for wavelength {wavelength}.")

    # Example: DEMA (ensure data_dema is a Pandas DataFrame)
    if isinstance(data_dema, pd.DataFrame) and f"{wavelength}_BC_DEMA" in data_dema.columns:
        timebase = data_dema['Timebase (s)'].iloc[0] if 'Timebase (s)' in data_dema.columns and not data_dema.empty else 60
        alpha = 0.125 # Default, apply your logic
        N_dema = 15 # Default N for DEMA if alpha is based on N=(2/alpha)-1
        if timebase == 1: N_dema = 300 / timebase
        elif timebase == 5: N_dema = 300 / timebase
        elif timebase == 60: N_dema = 900 / timebase
        if N_dema > 0 : alpha = 2 / (N_dema + 1)
        
        dema_equiv_window = int(2/alpha - 1) if alpha > 0 else 1
        dema_points_pd = pd.Series([dema_equiv_window] * len(data_dema))
        dema_stats_dict = {"mean": float(dema_equiv_window), "median": float(dema_equiv_window), 
                           "min": float(dema_equiv_window), "max": float(dema_equiv_window), "std": 0.0}
        methods.append(('DEMA', dema_points_pd, dema_stats_dict))
    else:
        print(f"Warning: DEMA data or BC column not found/valid for wavelength {wavelength}.")
    # --- End of Populating 'methods' ---

    if not methods:
        print(f"No time averaging data available for analysis for wavelength {wavelength}")
        return None

    # Create DataFrame for display using Pandas (transposed view)
    display_data_for_pd = {"Metric": ["Mean", "Median", "Min", "Max", "Std Dev"]}
    for name, _, stats_dict_item in methods:
        display_data_for_pd[name] = [
            stats_dict_item["mean"], stats_dict_item["median"],
            stats_dict_item["min"], stats_dict_item["max"], stats_dict_item["std"]
        ]
    df_for_display_pd = pd.DataFrame(display_data_for_pd).set_index("Metric")
    
    print(f"\nSummary statistics of window sizes for {wavelength} wavelength:")
    display(df_for_display_pd.style.format("{:.2f}", na_rep="N/A"))


    # Plotting logic (histograms, ECDF)
    plt.figure(figsize=(12, 6))
    has_ona_for_hist = any(name == 'ONA' for name, _, _ in methods)
    
    for name, points_series_for_plotting_pd, stats in methods:
        if name == 'ONA' and not points_series_for_plotting_pd.empty:
            plt.hist(points_series_for_plotting_pd, bins=30, alpha=0.7, label=f'ONA (adaptive, mean={stats["mean"]:.1f})')
        elif name == 'CMA':
            plt.axvline(stats["mean"], color='red', linestyle='dashed', linewidth=2, label=f'CMA (fixed, window={int(stats["mean"])})')
        elif name == 'DEMA':
            plt.axvline(stats["mean"], color='green', linestyle='dashed', linewidth=2, label=f'DEMA (equiv. window={int(stats["mean"])})')
    
    plt.xlabel('Number of Points Averaged')
    plt.ylabel('Frequency' if has_ona_for_hist else 'Density (Conceptual for fixed windows)')
    plt.title(f'Averaging Window Size Comparison - {wavelength} Wavelength')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    # ECDF plot for ONA
    ona_points_data_for_ecdf = None
    for name, points_pd, _ in methods:
        if name == 'ONA': 
            ona_points_data_for_ecdf = points_pd.sort_values()
            break
            
    if ona_points_data_for_ecdf is not None and not ona_points_data_for_ecdf.empty:
        plt.figure(figsize=(10, 6))
        y_ecdf = np.arange(1, len(ona_points_data_for_ecdf) + 1) / len(ona_points_data_for_ecdf)
        plt.plot(ona_points_data_for_ecdf, y_ecdf, marker='.', linestyle='none')
        plt.xlabel('Number of Points Averaged (ONA)')
        plt.ylabel('ECDF')
        plt.title(f'ECDF of ONA Adaptive Window Size - {wavelength} Wavelength')
        plt.grid(True, alpha=0.3)
        if ona_points_data_for_ecdf.min() > 0 and (ona_points_data_for_ecdf.max() / ona_points_data_for_ecdf.min() > 100 if ona_points_data_for_ecdf.min() != 0 else False) :
             plt.xscale('log')
        plt.tight_layout()
        plt.show()

    # Create Polars DataFrame for returning (with 'Method' as a column)
    # Ensure all stats are float or can be handled by Polars as nulls if np.nan
    df_for_return = pl.DataFrame({
        "Method": [m[0] for m in methods],
        "Mean": [m[2]["mean"] for m in methods],
        "Median": [m[2]["median"] for m in methods],
        "Min": [m[2]["min"] for m in methods],
        "Max": [m[2]["max"] for m in methods],
        "Std Dev": [m[2]["std"] for m in methods]
    }, schema={ # Define schema to handle potential NaNs correctly as Float64
        "Method": pl.Utf8, "Mean": pl.Float64, "Median": pl.Float64,
        "Min": pl.Float64, "Max": pl.Float64, "Std Dev": pl.Float64
    })
    
    return df_for_return

# %%
# Analyze time averaging for each wavelength
for wavelength in ['Blue', 'IR']:
    print(f"\nTime averaging analysis for {wavelength} wavelength:")
    analyze_time_averaging(processed_data_ona, processed_data_cma, processed_data_dema, wavelength)


# %% [markdown]
# ## 14. Comparing Signal Preservation
# 
# Let's examine how well each method preserves important signal features by looking at cross-correlation and lag.

# %%
def analyze_signal_preservation(data_raw, data_ona, data_cma, data_dema, wavelength='Blue'):
    """
    Analyze how well each method preserves important signal features
    
    Parameters:
    -----------
    data_raw : pandas.DataFrame
        DataFrame with raw data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed data
    wavelength : str
        Which wavelength to analyze
    """
    # Identify columns
    bc_col = f"{wavelength} BCc"
    ona_col = f"{wavelength}_BC_ONA"
    cma_col = f"{wavelength}_BC_CMA"
    dema_col = f"{wavelength}_BC_DEMA"
    
    # Check which methods are available
    methods = []
    if bc_col in data_raw.columns:
        # Convert to Polars for standardization
        pl_raw = pl.from_pandas(data_raw[[bc_col]])
        raw_mean = pl_raw.select(pl.col(bc_col).mean())[0, 0]
        raw_std = pl_raw.select(pl.col(bc_col).std())[0, 0]
        
        # Standardize the raw data using Polars
        pl_raw_std = pl_raw.with_columns(
            ((pl.col(bc_col) - raw_mean) / raw_std).alias("std_value")
        )
        
        # Convert back to numpy for correlation calculation
        raw_std_values = pl_raw_std["std_value"].fill_null(0).to_numpy()
        
        # Process each method
        if ona_col in data_ona.columns:
            pl_ona = pl.from_pandas(data_ona[[ona_col]])
            ona_mean = pl_ona.select(pl.col(ona_col).mean())[0, 0]
            ona_std = pl_ona.select(pl.col(ona_col).std())[0, 0]
            
            pl_ona_std = pl_ona.with_columns(
                ((pl.col(ona_col) - ona_mean) / ona_std).alias("std_value")
            )
            
            ona_std_values = pl_ona_std["std_value"].fill_null(0).to_numpy()
            methods.append(('ONA', ona_std_values))
            
        if cma_col in data_cma.columns:
            pl_cma = pl.from_pandas(data_cma[[cma_col]])
            cma_mean = pl_cma.select(pl.col(cma_col).mean())[0, 0]
            cma_std = pl_cma.select(pl.col(cma_col).std())[0, 0]
            
            pl_cma_std = pl_cma.with_columns(
                ((pl.col(cma_col) - cma_mean) / cma_std).alias("std_value")
            )
            
            cma_std_values = pl_cma_std["std_value"].fill_null(0).to_numpy()
            methods.append(('CMA', cma_std_values))
            
        if dema_col in data_dema.columns:
            pl_dema = pl.from_pandas(data_dema[[dema_col]])
            dema_mean = pl_dema.select(pl.col(dema_col).mean())[0, 0]
            dema_std = pl_dema.select(pl.col(dema_col).std())[0, 0]
            
            pl_dema_std = pl_dema.with_columns(
                ((pl.col(dema_col) - dema_mean) / dema_std).alias("std_value")
            )
            
            dema_std_values = pl_dema_std["std_value"].fill_null(0).to_numpy()
            methods.append(('DEMA', dema_std_values))
    else:
        print(f"Raw data for {wavelength} wavelength not available")
        return
    
    if len(methods) == 0:
        print("No processed data available for analysis")
        return
    
    # Calculate cross-correlation and find peak for each method
    results = {}
    lags = {}
    peak_corrs = {}
    
    max_lag = 20  # Maximum lag to consider (in data points)
    
    for name, processed_std in methods:
        # Calculate cross-correlation
        xcorr = np.correlate(raw_std_values, processed_std, mode='full')
        
        # Calculate the midpoint
        mid = len(xcorr) // 2
        
        # Extract the central portion of the cross-correlation
        lag_range = max_lag * 2 + 1
        central_xcorr = xcorr[mid - max_lag:mid + max_lag + 1]
        lags_array = np.arange(-max_lag, max_lag + 1)
        
        # Find the peak correlation and its lag
        peak_idx = np.argmax(central_xcorr)
        peak_lag = lags_array[peak_idx]
        peak_corr = central_xcorr[peak_idx]
        
        # Store results
        results[name] = {
            'xcorr': central_xcorr,
            'lags': lags_array
        }
        lags[name] = peak_lag
        peak_corrs[name] = peak_corr
    
    # Plot cross-correlation for each method
    plt.figure(figsize=(10, 6))
    
    for name, result in results.items():
        if name == 'ONA':
            plt.plot(result['lags'], result['xcorr'], 'b-', label=f'ONA (lag={lags[name]})')
        elif name == 'CMA':
            plt.plot(result['lags'], result['xcorr'], 'r-', label=f'CMA (lag={lags[name]})')
        elif name == 'DEMA':
            plt.plot(result['lags'], result['xcorr'], 'g-', label=f'DEMA (lag={lags[name]})')
    
    plt.axvline(x=0, color='k', linestyle='--', alpha=0.5)
    plt.xlabel('Lag (data points)')
    plt.ylabel('Cross-Correlation')
    plt.title(f'Cross-Correlation with Raw Data - {wavelength} Wavelength')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

    # Create the summary Polars DataFrame
    summary = pl.DataFrame({
        'Method': list(results.keys()), # Ensure 'results' is correctly populated
        'Lag (data points)': [lags[name] for name in results.keys()], # Ensure 'lags' is correctly populated
        'Peak Correlation': [peak_corrs[name] for name in results.keys()] # Ensure 'peak_corrs' is correctly populated
    })
    
    print(f"\nSignal preservation metrics for {wavelength} wavelength:")
    display(summary) # You are already displaying it

    return summary   # <<<< ADD THIS LINE or ensure it's the last executed statement


# %%
# Analyze signal preservation for each wavelength
for wavelength in ['Blue', 'IR']:
    print(f"\nSignal preservation analysis for {wavelength} wavelength:")
    analyze_signal_preservation(data, processed_data_ona, processed_data_cma, processed_data_dema, wavelength)


# %% [markdown]
# ## 15. Comparative Analysis Summary
#  
# Now let's summarize the performance of all three methods across key metrics.

# %%
def create_method_comparison_table(
    wavelength: str,
    summary_metrics_all_wavelengths_pd: pd.DataFrame, # Pandas DF from evaluate_processing
    signal_preservation_pl: pl.DataFrame = None,      # Polars DF from analyze_signal_preservation
    time_averaging_pl: pl.DataFrame = None,           # Polars DF from analyze_time_averaging
    sa_stability_by_method: dict = None               # Dict {'ONA': std_dev, ...}
):
    """
    Create a quantitative comparison table for processing methods for a specific wavelength.
    """
    methods_to_compare = ['ONA', 'CMA', 'DEMA'] # Define the methods you are comparing
    
    # Define the metrics to be included in the table
    table_metrics = [
        'Negative Value Reduction (%)',
        'Noise Reduction Factor (x)',
        'Correlation with Raw Data',
        'Signal Lag (data points)',
        'Peak Cross-Correlation Value (Standardized)', # Renamed for clarity
        'Mean Points Averaged (Window Size)',
        'Std Dev of BB% (SA Stability)'
    ]
    
    # Initialize data structure for Polars DataFrame
    table_data_dict = {'Metric': table_metrics}
    for method in methods_to_compare:
        table_data_dict[method] = ['N/A'] * len(table_metrics)
        
    comparison_pl = pl.DataFrame(table_data_dict)

    # Ensure summary_metrics_all_wavelengths_pd is a Pandas DataFrame before reset_index
    if not isinstance(summary_metrics_all_wavelengths_pd, pd.DataFrame):
        print(f"Warning: summary_metrics_all_wavelengths_pd is not a Pandas DataFrame for wavelength {wavelength}. Skipping some metrics.")
        summary_pl = None
    else:
        summary_pl = pl.from_pandas(summary_metrics_all_wavelengths_pd.reset_index())

    for method_name in methods_to_compare:
        # --- Populate metrics from summary_pl (derived from evaluate_processing) ---
        if summary_pl is not None:
            summary_key_for_method = f"{wavelength}_{method_name}" # e.g., "Blue_ONA"
            filtered_summary_rows = summary_pl.filter(pl.col('index') == summary_key_for_method)
            
            if filtered_summary_rows.height > 0:
                row_data = filtered_summary_rows.row(0, named=True) # Get row as dict

                # Negative Value Reduction
                if 'Negative reduction' in row_data and row_data['Negative reduction'] is not None:
                    val = row_data['Negative reduction']
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Negative Value Reduction (%)')
                        .then(pl.lit(f"{val * 100:.1f}")) # Assuming val is fraction 0-1
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )
                # Noise Reduction Factor
                if 'Noise reduction factor' in row_data and row_data['Noise reduction factor'] is not None:
                    val = row_data['Noise reduction factor']
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Noise Reduction Factor (x)')
                        .then(pl.lit(f"{val:.1f}x"))
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )
                # Correlation with Raw Data
                if 'Correlation with raw' in row_data and row_data['Correlation with raw'] is not None:
                    val = row_data['Correlation with raw']
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Correlation with Raw Data')
                        .then(pl.lit(f"{val:.3f}"))
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )

        # --- Populate metrics from signal_preservation_pl ---
        if signal_preservation_pl is not None:
            signal_row_data = signal_preservation_pl.filter(pl.col('Method') == method_name)
            if signal_row_data.height > 0:
                row_data = signal_row_data.row(0, named=True)
                # Signal Lag
                if 'Lag (data points)' in row_data and row_data['Lag (data points)'] is not None:
                    val = row_data['Lag (data points)']
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Signal Lag (data points)')
                        .then(pl.lit(str(val)))
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )
                # Peak Cross-Correlation Value
                if 'Peak Correlation' in row_data and row_data['Peak Correlation'] is not None:
                    val = row_data['Peak Correlation']
                    # Note: This value from analyze_signal_preservation is not a normalized correlation coefficient (-1 to 1).
                    # It's the max value of the cross-correlation of standardized series.
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Peak Cross-Correlation Value (Standardized)')
                        .then(pl.lit(f"{val:.3e}")) # Use scientific notation if values can be large/small
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )
        
        # --- Populate metrics from time_averaging_pl ---
        if time_averaging_pl is not None:
            time_avg_row_data = time_averaging_pl.filter(pl.col('Method') == method_name)
            if time_avg_row_data.height > 0:
                row_data = time_avg_row_data.row(0, named=True)
                # Mean Points Averaged (Window Size)
                if 'Mean' in row_data and row_data['Mean'] is not None: # 'Mean' column from analyze_time_averaging
                    val = row_data['Mean']
                    comparison_pl = comparison_pl.with_columns(
                        pl.when(pl.col('Metric') == 'Mean Points Averaged (Window Size)')
                        .then(pl.lit(f"{val:.1f}"))
                        .otherwise(pl.col(method_name))
                        .alias(method_name)
                    )

        # --- Populate metrics from sa_stability_by_method ---
        # This metric is global per method, not per wavelength of BC processing, but shown in each table for completeness.
        if sa_stability_by_method and method_name in sa_stability_by_method:
            val = sa_stability_by_method[method_name]
            if val is not None and not np.isnan(val):
                comparison_pl = comparison_pl.with_columns(
                    pl.when(pl.col('Metric') == 'Std Dev of BB% (SA Stability)')
                    .then(pl.lit(f"{val:.2f}"))
                    .otherwise(pl.col(method_name))
                    .alias(method_name)
                )
            
    comparison_pl = comparison_pl.sort("Metric")
    return comparison_pl

# %% [markdown]
# ## 16. Conclusions and Method Recommendations
#  
# This notebook implemented and compared three post-processing algorithms for aethalometer data: ONA, CMA, and DEMA.

# %%
print("\n===== Quantitative Method Comparison Tables (Wavelength Specific) =====")

# Ensure summary_table (pandas DataFrame from evaluate_processing in Section 9) is available
# Ensure sa_stability_metrics (dict from Step 2 of previous instructions) is available

# Initialize stores if not run interactively section by section
if 'time_averaging_results_store' not in locals() or not isinstance(time_averaging_results_store, dict):
    print("DEBUG: Initializing `time_averaging_results_store` dictionary.")
    time_averaging_results_store = {}

if 'signal_preservation_results_store' not in locals() or not isinstance(signal_preservation_results_store, dict):
    print("DEBUG: Initializing `signal_preservation_results_store` dictionary.")
    signal_preservation_results_store = {}

# Fallback logic if stores are empty (e.g., first run or non-sequential execution)
if not time_averaging_results_store: # Check if dict is empty
    print("Warning: `time_averaging_results_store` is empty or not found. Running analysis now to populate.")
    for wl_ana in ['Blue', 'IR']:
        print(f"DEBUG: Running analyze_time_averaging for {wl_ana} in fallback.")
        if 'processed_data_ona' in locals() and 'processed_data_cma' in locals() and 'processed_data_dema' in locals():
            df_time_avg = analyze_time_averaging(processed_data_ona, processed_data_cma, processed_data_dema, wl_ana)
            if df_time_avg is not None:
                print(f"DEBUG: Storing time_avg_df for {wl_ana}. Is None: {df_time_avg is None}. Shape: {df_time_avg.shape if df_time_avg is not None else 'N/A'}")
                time_averaging_results_store[wl_ana] = df_time_avg
            else:
                print(f"DEBUG: analyze_time_averaging for {wl_ana} returned None.")
        else:
            print(f"DEBUG: Skipped analyze_time_averaging for {wl_ana} in fallback due to missing processed dataframes.")

if not signal_preservation_results_store: # Check if dict is empty
    print("Warning: `signal_preservation_results_store` is empty or not found. Running analysis now to populate.")
    for wl_ana in ['Blue', 'IR']:
        print(f"DEBUG: Running analyze_signal_preservation for {wl_ana} in fallback.")
        if 'data' in locals() and 'processed_data_ona' in locals() and 'processed_data_cma' in locals() and 'processed_data_dema' in locals():
            df_signal = analyze_signal_preservation(data, processed_data_ona, processed_data_cma, processed_data_dema, wl_ana)
            if df_signal is not None:
                print(f"DEBUG: Storing signal_pres_df for {wl_ana}. Is None: {df_signal is None}. Shape: {df_signal.shape if df_signal is not None else 'N/A'}")
                signal_preservation_results_store[wl_ana] = df_signal
            else:
                print(f"DEBUG: analyze_signal_preservation for {wl_ana} returned None.")
        else:
             print(f"DEBUG: Skipped analyze_signal_preservation for {wl_ana} in fallback due to missing dataframes.")


if 'summary_table' not in locals() or not isinstance(summary_table, pd.DataFrame):
    print("Error: `summary_table` (Pandas DataFrame from Section 9) is not available. Cannot generate comparison tables.")
elif 'sa_stability_metrics' not in locals() or not isinstance(sa_stability_metrics, dict):
    print("Error: `sa_stability_metrics` (dict) is not available. Cannot generate comparison tables.") # This was your error
else:
    all_comparison_tables = {}
    for wl_to_compare in ['Blue', 'IR']:
        print(f"\n----- Comparison Table for {wl_to_compare} Wavelength -----")
        
        current_signal_metrics_pl = signal_preservation_results_store.get(wl_to_compare)
        current_time_avg_metrics_pl = time_averaging_results_store.get(wl_to_compare)
        
        print(f"DEBUG: For table [{wl_to_compare}]: current_time_avg_metrics_pl is None? {current_time_avg_metrics_pl is None}")
        if current_time_avg_metrics_pl is not None:
            print("DEBUG: current_time_avg_metrics_pl head:")
            display(current_time_avg_metrics_pl.head())
        
        print(f"DEBUG: For table [{wl_to_compare}]: current_signal_metrics_pl is None? {current_signal_metrics_pl is None}")
        if current_signal_metrics_pl is not None:
            print("DEBUG: current_signal_metrics_pl head:")
            display(current_signal_metrics_pl.head())
        
        comparison_table_for_wl = create_method_comparison_table(
            wavelength=wl_to_compare,
            summary_metrics_all_wavelengths_pd=summary_table, 
            signal_preservation_pl=current_signal_metrics_pl,
            time_averaging_pl=current_time_avg_metrics_pl,
            sa_stability_by_method=sa_stability_metrics
        )
        display(comparison_table_for_wl)
        all_comparison_tables[wl_to_compare] = comparison_table_for_wl

# %% [markdown]
# ## 17. Save Processed Data

# %%
def save_processed_data(data, processed_data_ona, processed_data_cma, processed_data_dema, output_file):
    """
    Save processed data with all methods to a CSV file using Polars for better performance
    
    Parameters:
    -----------
    data : pandas.DataFrame
        Original data
    processed_data_ona : pandas.DataFrame
        ONA processed data
    processed_data_cma : pandas.DataFrame
        CMA processed data
    processed_data_dema : pandas.DataFrame
        DEMA processed data
    output_file : str
        Output file path
    
    Returns:
    --------
    pl_combined : polars.DataFrame
        Combined DataFrame with all processed data
    """
    # Start with base data as Polars DataFrame
    pl_combined = pl.from_pandas(data)
    
    # Convert processing results to Polars DataFrames
    pl_ona = pl.from_pandas(processed_data_ona)
    pl_cma = pl.from_pandas(processed_data_cma)
    pl_dema = pl.from_pandas(processed_data_dema)
    
    # Add ONA columns - only those not already in the combined data
    ona_cols = [col for col in pl_ona.columns if col not in pl_combined.columns]
    for col in ona_cols:
        pl_combined = pl_combined.with_columns(
            pl_ona.select(pl.col(col)).to_series()
        )
    
    # Add CMA columns - only those not already in the combined data
    cma_cols = [col for col in pl_cma.columns if col not in pl_combined.columns]
    for col in cma_cols:
        pl_combined = pl_combined.with_columns(
            pl_cma.select(pl.col(col)).to_series()
        )
    
    # Add DEMA columns - only those not already in the combined data
    dema_cols = [col for col in pl_dema.columns if col not in pl_combined.columns]
    for col in dema_cols:
        pl_combined = pl_combined.with_columns(
            pl_dema.select(pl.col(col)).to_series()
        )
    
    # Save to CSV directly using Polars - more efficient for large datasets
    pl_combined.write_csv(output_file)
    print(f"Processed data from all methods saved to {output_file}")
    
    # Return the combined DataFrame in case it's needed for further processing
    return pl_combined

# Save the processed data
output_file = "aethalometer_data_all_methods.csv"
combined_data = save_processed_data(data, processed_data_ona, processed_data_cma, processed_data_dema, output_file)

# Optional: Print summary of what was saved
print("\nSummary of saved data:")
print(f"Total columns: {len(combined_data.columns)}")
print(f"Total rows: {combined_data.height}")

# Count columns by processing method
ona_cols = [col for col in combined_data.columns if '_BC_ONA' in col or '_points_averaged' in col]
cma_cols = [col for col in combined_data.columns if '_BC_CMA' in col]
dema_cols = [col for col in combined_data.columns if '_BC_DEMA' in col or '_EMA' in col]
sa_cols = [col for col in combined_data.columns if 'BB_Percent' in col or 'BC_WB' in col or 'BC_FF' in col]

print(f"Original data columns: {len(combined_data.columns) - len(ona_cols) - len(cma_cols) - len(dema_cols) - len(sa_cols)}")
print(f"ONA-related columns: {len(ona_cols)}")
print(f"CMA-related columns: {len(cma_cols)}")
print(f"DEMA-related columns: {len(dema_cols)}")
print(f"Source apportionment columns: {len(sa_cols)}")

# %% [markdown]
# ## 18. Correlation Slope Analysis Between Methods (Blue and IR)
# This section calculates the slope of linear regression fits between processing methods,
# focusing on Blue and IR BC values. This can help understand systematic biases between methods.

# %%
def calculate_correlation_slopes_np(data_dict, wavelength='Blue'):
    """
    Calculate slopes of linear regression fits using numpy between pairs of processing methods.
    
    Parameters:
    -----------
    data_dict : dict
        Dictionary with method names as keys and pandas Series as values
    wavelength : str
        Wavelength label
    
    Returns:
    --------
    pandas.DataFrame
    """
    methods = list(data_dict.keys())
    results = []

    for i in range(len(methods)):
        for j in range(i + 1, len(methods)):
            method_x = methods[i]
            method_y = methods[j]
            x = data_dict[method_x].values
            y = data_dict[method_y].values

            # Remove NaNs
            valid = ~np.isnan(x) & ~np.isnan(y)
            x_valid = x[valid]
            y_valid = y[valid]

            if len(x_valid) > 1:
                # Fit: y = m*x + b
                slope, intercept = np.polyfit(x_valid, y_valid, 1)
                # R² calculation
                y_pred = slope * x_valid + intercept
                ss_res = np.sum((y_valid - y_pred) ** 2)
                ss_tot = np.sum((y_valid - np.mean(y_valid)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot != 0 else np.nan

                results.append({
                    'Wavelength': wavelength,
                    'X Method': method_x,
                    'Y Method': method_y,
                    'Slope': slope,
                    'Intercept': intercept,
                    'R²': r2
                })

    return pd.DataFrame(results)

# %%
# Run correlation slope analysis for both wavelengths
for wl in ['Blue', 'IR']:
    print(f"\n=== Correlation Slope Analysis: {wl} Wavelength ===")

    method_cols = {
        'Raw': data[f"{wl} BCc"],
        'ONA': processed_data_ona.get(f"{wl}_BC_ONA", pd.Series(index=data.index, dtype=float)),
        'CMA': processed_data_cma.get(f"{wl}_BC_CMA", pd.Series(index=data.index, dtype=float)),
        'DEMA': processed_data_dema.get(f"{wl}_BC_DEMA", pd.Series(index=data.index, dtype=float))
    }

    # Drop entirely empty series
    method_cols = {k: v for k, v in method_cols.items() if not v.isnull().all()}

    # Run analysis
    corr_slopes_df = calculate_correlation_slopes_np(method_cols, wavelength=wl)
    display(corr_slopes_df.style.format({"Slope": "{:.3f}", "Intercept": "{:.2f}", "R²": "{:.3f}"}))

# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def remove_outliers(x, y, lower=0.002, upper=0.998):
    """
    Remove outliers based on joint percentiles of x and y.
    """
    x_low, x_high = np.nanpercentile(x, [100 * lower, 100 * upper])
    y_low, y_high = np.nanpercentile(y, [100 * lower, 100 * upper])
    valid = (x >= x_low) & (x <= x_high) & (y >= y_low) & (y <= y_high)
    return x[valid], y[valid]

def calculate_correlation_slopes_np(data_dict, wavelength='Blue', remove_outliers_flag=True):
    """
    Calculate slopes of linear regression fits using numpy between pairs of processing methods.
    Optionally removes outliers before fitting.

    Parameters:
    -----------
    data_dict : dict
        Dictionary with method names as keys and pandas Series as values
    wavelength : str
        Wavelength label
    remove_outliers_flag : bool
        Whether to remove outliers before regression

    Returns:
    --------
    pandas.DataFrame
    """
    methods = list(data_dict.keys())
    results = []

    for i in range(len(methods)):
        for j in range(i + 1, len(methods)):
            method_x = methods[i]
            method_y = methods[j]
            x = data_dict[method_x].values
            y = data_dict[method_y].values

            # Remove NaNs
            valid = ~np.isnan(x) & ~np.isnan(y)
            x_valid = x[valid]
            y_valid = y[valid]

            if remove_outliers_flag:
                x_valid, y_valid = remove_outliers(x_valid, y_valid)

            if len(x_valid) > 1:
                slope, intercept = np.polyfit(x_valid, y_valid, 1)
                y_pred = slope * x_valid + intercept
                ss_res = np.sum((y_valid - y_pred) ** 2)
                ss_tot = np.sum((y_valid - np.mean(y_valid)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot != 0 else np.nan

                results.append({
                    'Wavelength': wavelength,
                    'X Method': method_x,
                    'Y Method': method_y,
                    'Slope': slope,
                    'Intercept': intercept,
                    'R²': r2
                })

    return pd.DataFrame(results)

def plot_correlation_slope_fits(data_dict, slope_df, wavelength, cols=3):
    """
    Plot scatter plots with regression lines for all method pairs based on slope_df.

    Parameters:
    -----------
    data_dict : dict
        Dictionary with method names as keys and pandas Series as values
    slope_df : pd.DataFrame
        DataFrame returned by calculate_correlation_slopes_np
    wavelength : str
        'Blue' or 'IR' for title
    cols : int
        Number of columns in subplot grid
    """
    pairs = slope_df[['X Method', 'Y Method']].values
    num_plots = len(pairs)
    rows = int(np.ceil(num_plots / cols))

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 5 * rows), squeeze=False)

    for idx, (x_method, y_method) in enumerate(pairs):
        ax = axes[idx // cols][idx % cols]
        x = data_dict[x_method]
        y = data_dict[y_method]

        mask = x.notna() & y.notna()
        x_valid = x[mask]
        y_valid = y[mask]

        x_valid, y_valid = remove_outliers(x_valid.values, y_valid.values)

        slope = slope_df.loc[(slope_df['X Method'] == x_method) &
                             (slope_df['Y Method'] == y_method), 'Slope'].values[0]
        intercept = slope_df.loc[(slope_df['X Method'] == x_method) &
                                 (slope_df['Y Method'] == y_method), 'Intercept'].values[0]
        r2 = slope_df.loc[(slope_df['X Method'] == x_method) &
                          (slope_df['Y Method'] == y_method), 'R²'].values[0]

        ax.scatter(x_valid, y_valid, alpha=0.6, label='Data')
        x_line = np.linspace(np.min(x_valid), np.max(x_valid), 100)
        y_line = slope * x_line + intercept
        ax.plot(x_line, y_line, 'r--', label=f'y={slope:.2f}x+{intercept:.2f}, R²={r2:.3f}')
        ax.plot(x_line, x_line, 'k:', label='1:1 Line')

        ax.set_xlabel(f'{x_method}')
        ax.set_ylabel(f'{y_method}')
        ax.set_title(f'{wavelength} BC: {y_method} vs {x_method}')
        ax.legend()
        ax.grid(True)

    for idx in range(num_plots, rows * cols):
        fig.delaxes(axes[idx // cols][idx % cols])

    fig.tight_layout()
    plt.show()


# %%
# Run for both wavelengths
for wl in ['Blue', 'IR']:
    print(f"\n=== Correlation Slope Analysis: {wl} Wavelength ===")

    method_cols = {
        'Raw': data[f"{wl} BCc"],
        'ONA': processed_data_ona.get(f"{wl}_BC_ONA", pd.Series(index=data.index, dtype=float)),
        'CMA': processed_data_cma.get(f"{wl}_BC_CMA", pd.Series(index=data.index, dtype=float)),
        'DEMA': processed_data_dema.get(f"{wl}_BC_DEMA", pd.Series(index=data.index, dtype=float))
    }

    method_cols = {k: v for k, v in method_cols.items() if not v.isnull().all()}

    corr_slopes_df = calculate_correlation_slopes_np(method_cols, wavelength=wl)
    display(corr_slopes_df.style.format({"Slope": "{:.3f}", "Intercept": "{:.2f}", "R²": "{:.3f}"}))

    if not corr_slopes_df.empty:
        plot_correlation_slope_fits(method_cols, corr_slopes_df, wavelength=wl, cols=3)

# %% [markdown]
# ## 19. Cross Plots of Daily Means Between Methods (Blue and IR)
# This section creates cross plots (scatter plots) of daily mean BC values to visually compare
# biases and relationships between methods.

# %%
def plot_daily_mean_crossplots(data_dict, method_pairs, wavelength, time_col='Time (UTC)', cols=3):
    """
    Plot daily mean cross plots in a grid layout.

    Parameters:
    -----------
    data_dict : dict
        Dictionary with method names as keys and pandas Series as values
    method_pairs : list of tuples
        List of (x_method, y_method) pairs to plot
    wavelength : str
        Label for plot titles
    time_col : str
        Column name for datetime index
    cols : int
        Number of columns in grid layout
    """
    import matplotlib.pyplot as plt

    # Combine into DataFrame and resample to daily
    df = pd.DataFrame({k: v for k, v in data_dict.items()})
    df[time_col] = pd.to_datetime(data[time_col])
    df.set_index(time_col, inplace=True)
    df_daily = df.resample('D').mean()

    num_plots = len(method_pairs)
    rows = int(np.ceil(num_plots / cols))

    fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 5 * rows), squeeze=False)

    for idx, (x_method, y_method) in enumerate(method_pairs):
        ax = axes[idx // cols][idx % cols]
        x = df_daily[x_method]
        y = df_daily[y_method]
        mask = x.notna() & y.notna()
        x_clean = x[mask]
        y_clean = y[mask]

        if len(x_clean) > 1:
            slope, intercept = np.polyfit(x_clean, y_clean, 1)
            y_fit = slope * x_clean + intercept
            r2 = 1 - np.sum((y_clean - y_fit) ** 2) / np.sum((y_clean - np.mean(y_clean)) ** 2)

            ax.scatter(x_clean, y_clean, alpha=0.6, label='Daily Means')
            x_line = np.linspace(x_clean.min(), x_clean.max(), 100)
            y_line = slope * x_line + intercept
            ax.plot(x_line, y_line, 'r--', label=f'y={slope:.2f}x+{intercept:.2f}, R²={r2:.3f}')
        else:
            ax.text(0.5, 0.5, 'Not enough data', ha='center')

        ax.plot([x_clean.min(), x_clean.max()],
                [x_clean.min(), x_clean.max()], 'k:', label='1:1 Line')
        ax.set_xlabel(f'{x_method} (daily mean)')
        ax.set_ylabel(f'{y_method} (daily mean)')
        ax.set_title(f'{wavelength} BC: {y_method} vs {x_method}')
        ax.legend()
        ax.grid(True)

    # Hide unused subplots
    for idx in range(num_plots, rows * cols):
        fig.delaxes(axes[idx // cols][idx % cols])

    fig.tight_layout()
    plt.show()


# %%
# Example: Cross plots for Blue wavelength
for wl in ['Blue', 'IR']:
    print(f"\n=== Cross Plot of Daily Means: {wl} Wavelength ===")

    method_cols = {
        'Raw': data[f"{wl} BCc"],
        'ONA': processed_data_ona.get(f"{wl}_BC_ONA", pd.Series(index=data.index, dtype=float)),
        'CMA': processed_data_cma.get(f"{wl}_BC_CMA", pd.Series(index=data.index, dtype=float)),
        'DEMA': processed_data_dema.get(f"{wl}_BC_DEMA", pd.Series(index=data.index, dtype=float))
    }
    method_cols = {k: v for k, v in method_cols.items() if not v.isnull().all()}

    method_list = list(method_cols.keys())
    method_pairs = [(method_list[i], method_list[j]) 
                    for i in range(len(method_list)) 
                    for j in range(i + 1, len(method_list))]

    # Now using grid layout (e.g., 3 columns)
    plot_daily_mean_crossplots(method_cols, method_pairs, wl, cols=3)


# %% [markdown]
# ## 20. Daily Average Analysis of BC Processing Methods
#   
# This section analyzes how each processing method performs when data is aggregated to daily averages, which is important for health studies, regulatory compliance, and comparison with other air quality datasets.
# 

# %% [markdown]
# ### 20.1 Calculate Daily Averages

# %%
def calculate_daily_averages(data, data_ona, data_cma, data_dema, wavelengths=['Blue', 'IR']):
    """
    Calculate daily averages for all processing methods
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame with raw data
    data_ona : pandas.DataFrame
        DataFrame with ONA processed data
    data_cma : pandas.DataFrame
        DataFrame with CMA processed data
    data_dema : pandas.DataFrame
        DataFrame with DEMA processed data
    wavelengths : list of str
        Wavelengths to process, default ['Blue', 'IR']
        
    Returns:
    --------
    dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
    """
    daily_dfs = {}
    
    for wavelength in wavelengths:
        # Gather column names
        bc_col = f"{wavelength} BCc"
        ona_col = f"{wavelength}_BC_ONA"
        cma_col = f"{wavelength}_BC_CMA"
        dema_col = f"{wavelength}_BC_DEMA"
        
        # Create combined dataframe with all methods
        methods_data = {}
        
        # Add raw data if available
        if bc_col in data.columns:
            methods_data['Raw'] = data[bc_col]
            
        # Add ONA data if available
        if ona_col in data_ona.columns:
            methods_data['ONA'] = data_ona[ona_col]
            
        # Add CMA data if available
        if cma_col in data_cma.columns:
            methods_data['CMA'] = data_cma[cma_col]
            
        # Add DEMA data if available
        if dema_col in data_dema.columns:
            methods_data['DEMA'] = data_dema[dema_col]
        
        if not methods_data:
            print(f"No data available for {wavelength} wavelength")
            continue
            
        # Create dataframe with datetime index
        df = pd.DataFrame(methods_data)
        df['Time (UTC)'] = pd.to_datetime(data['Time (UTC)'])
        df.set_index('Time (UTC)', inplace=True)
        
        # Calculate daily averages
        df_daily = df.resample('D').mean()
        
        # Add to dictionary
        daily_dfs[wavelength] = df_daily
    
    return daily_dfs

# Calculate daily averages for each wavelength
print("Calculating daily averages...")
daily_dfs = calculate_daily_averages(data, processed_data_ona, processed_data_cma, processed_data_dema)

# Print summary of daily data
for wavelength, df in daily_dfs.items():
    print(f"\nDaily average data summary for {wavelength} wavelength:")
    print(f"Date range: {df.index.min().date()} to {df.index.max().date()}")
    print(f"Number of days: {len(df)}")
    print(f"Methods available: {', '.join(df.columns)}")

# %% [markdown]
# ### 20.2 Visualize Daily Average Time Series

# %%
def plot_daily_timeseries(daily_dfs):
    """
    Plot daily average time series for all methods
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
    """
    for wavelength, df in daily_dfs.items():
        if df.empty:
            print(f"No data available for {wavelength} wavelength")
            continue
            
        # Plot time series
        plt.figure(figsize=(14, 8))
        
        for column in df.columns:
            if column == 'Raw':
                plt.plot(df.index, df[column], 'k-', alpha=0.6, label=column)
            elif column == 'ONA':
                plt.plot(df.index, df[column], 'b-', label=column)
            elif column == 'CMA':
                plt.plot(df.index, df[column], 'r-', label=column)
            elif column == 'DEMA':
                plt.plot(df.index, df[column], 'g-', label=column)
        
        plt.xlabel('Date')
        plt.ylabel(f'{wavelength} BC (ng/m³)')
        plt.title(f'Daily Average {wavelength} BC by Processing Method')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
        plt.gcf().autofmt_xdate()
        
        plt.tight_layout()
        plt.show()
        
        # Plot differences from Raw data
        if 'Raw' in df.columns and len(df.columns) > 1:
            plt.figure(figsize=(14, 8))
            
            for column in df.columns:
                if column != 'Raw':
                    diff = df[column] - df['Raw']
                    if column == 'ONA':
                        plt.plot(df.index, diff, 'b-', label=f'{column} - Raw')
                    elif column == 'CMA':
                        plt.plot(df.index, diff, 'r-', label=f'{column} - Raw')
                    elif column == 'DEMA':
                        plt.plot(df.index, diff, 'g-', label=f'{column} - Raw')
            
            plt.axhline(y=0, color='k', linestyle='--')
            plt.xlabel('Date')
            plt.ylabel(f'Difference in {wavelength} BC (ng/m³)')
            plt.title(f'Daily Average Difference from Raw Data - {wavelength} BC')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Format x-axis dates
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
            plt.gcf().autofmt_xdate()
            
            plt.tight_layout()
            plt.show()

# Plot the daily time series
print("Plotting daily average time series...")
plot_daily_timeseries(daily_dfs)


# %% [markdown]
# ### 20.3 Statistical Analysis of Daily Averages

# %%
def analyze_daily_statistics(daily_dfs):
    """
    Analyze statistical properties of daily averages by method
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
        
    Returns:
    --------
    dict of pandas.DataFrame
        Dictionary with wavelength as key and statistics DataFrame as value
    """
    stats_results = {}
    
    for wavelength, df in daily_dfs.items():
        print(f"\n===== Statistical Analysis of Daily Averages for {wavelength} Wavelength =====")
        
        # Calculate statistics
        stats_df = df.agg(['count', 'mean', 'std', 'min', 'max', 'median']).T
        stats_df['cv'] = stats_df['std'] / stats_df['mean'] * 100  # Coefficient of variation
        
        # Create box plot
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=df)
        
        plt.ylabel(f'{wavelength} BC (ng/m³)')
        plt.title(f'Distribution of Daily Average {wavelength} BC by Method')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        
        # Display statistics table
        print(f"\nStatistics for daily average {wavelength} BC by method:")
        display(stats_df.style.format({
            'count': '{:.0f}',
            'mean': '{:.2f}',
            'std': '{:.2f}',
            'min': '{:.2f}',
            'max': '{:.2f}',
            'median': '{:.2f}',
            'cv': '{:.2f}%'
        }))
        
        stats_results[wavelength] = stats_df
        
        # Calculate relative average differences from raw
        if 'Raw' in df.columns:
            rel_diff_df = pd.DataFrame(index=df.columns)
            for col in df.columns:
                if col != 'Raw':
                    rel_diff = ((df[col] - df['Raw']) / df['Raw'] * 100).mean()
                    rel_diff_df.loc[col, 'Mean Relative Difference from Raw (%)'] = rel_diff
            
            rel_diff_df = rel_diff_df.dropna()
            if not rel_diff_df.empty:
                print(f"\nAverage relative differences from raw data:")
                display(rel_diff_df.style.format('{:.2f}%'))
    
    return stats_results

# Analyze daily averages statistics
print("Analyzing statistical properties of daily averages...")
daily_stats = analyze_daily_statistics(daily_dfs)


# %% [markdown]
# ### 20.4 Method Comparison with Paired Differences Analysis

# %%
from scipy import stats
from sklearn.metrics import mean_absolute_percentage_error

def analyze_paired_differences(daily_dfs):
    """
    Analyze paired differences between methods
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
        
    Returns:
    --------
    dict of pandas.DataFrame
        Dictionary with wavelength as key and paired differences DataFrame as value
    """
    results = {}
    
    for wavelength, df in daily_dfs.items():
        print(f"\n===== Paired Differences Analysis for {wavelength} Wavelength =====")
        
        method_pairs = []
        for i, method1 in enumerate(df.columns):
            for method2 in df.columns[i+1:]:
                method_pairs.append((method1, method2))
        
        pair_results = []
        
        for method1, method2 in method_pairs:
            # Drop NaN values
            pair_df = df[[method1, method2]].dropna()
            
            if len(pair_df) < 2:
                print(f"Not enough data for {method1} vs {method2} comparison")
                continue
                
            # Calculate differences
            diff = pair_df[method2] - pair_df[method1]
            rel_diff = diff / pair_df[method1] * 100
            
            # Perform paired t-test
            t_stat, p_value = stats.ttest_rel(pair_df[method1], pair_df[method2])
            
            # Calculate mean absolute percentage error
            mape = mean_absolute_percentage_error(pair_df[method1], pair_df[method2]) * 100
            
            # Calculate correlation
            corr = pair_df[method1].corr(pair_df[method2])
            
            result = {
                'Method 1': method1,
                'Method 2': method2,
                'Mean Diff': diff.mean(),
                'Median Diff': diff.median(),
                'Mean Rel Diff (%)': rel_diff.mean(),
                'Median Rel Diff (%)': rel_diff.median(),
                'T-statistic': t_stat,
                'P-value': p_value,
                'Correlation': corr,
                'MAPE (%)': mape
            }
            
            pair_results.append(result)
        
        if pair_results:
            results[wavelength] = pd.DataFrame(pair_results)
            display(results[wavelength].style.format({
                'Mean Diff': '{:.2f}',
                'Median Diff': '{:.2f}',
                'Mean Rel Diff (%)': '{:.2f}',
                'Median Rel Diff (%)': '{:.2f}',
                'T-statistic': '{:.3f}',
                'P-value': '{:.4f}',
                'Correlation': '{:.3f}',
                'MAPE (%)': '{:.2f}'
            }))
        else:
            print(f"No valid pairs for analysis for {wavelength} wavelength")
    
    return results

# Analyze paired differences
print("Analyzing paired differences between methods...")
diff_results = analyze_paired_differences(daily_dfs)

# %% [markdown]
# ### 20.5 Method Agreement Analysis with Bland-Altman Plots

# %%
def create_bland_altman_plots(daily_dfs):
    """
    Create Bland-Altman plots to evaluate agreement between methods
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
    """
    for wavelength, df in daily_dfs.items():
        method_pairs = []
        for i, method1 in enumerate(df.columns):
            for method2 in df.columns[i+1:]:
                method_pairs.append((method1, method2))
        
        if not method_pairs:
            print(f"No method pairs available for {wavelength} wavelength")
            continue
        
        # Create a grid of Bland-Altman plots
        n_pairs = len(method_pairs)
        cols = min(3, n_pairs)
        rows = (n_pairs + cols - 1) // cols
        
        fig, axes = plt.subplots(rows, cols, figsize=(cols*5, rows*4), squeeze=False)
        
        for i, (method1, method2) in enumerate(method_pairs):
            row, col = divmod(i, cols)
            ax = axes[row, col]
            
            # Drop NaN values
            pair_df = df[[method1, method2]].dropna()
            
            if len(pair_df) < 2:
                ax.text(0.5, 0.5, f"Not enough data for\n{method1} vs {method2}", 
                         ha='center', va='center', transform=ax.transAxes)
                continue
            
            # Calculate mean and difference
            mean_values = (pair_df[method1] + pair_df[method2]) / 2
            diff_values = pair_df[method2] - pair_df[method1]
            
            # Calculate mean difference and limits of agreement
            mean_diff = diff_values.mean()
            std_diff = diff_values.std()
            upper_loa = mean_diff + 1.96 * std_diff
            lower_loa = mean_diff - 1.96 * std_diff
            
            # Create Bland-Altman plot
            ax.scatter(mean_values, diff_values, alpha=0.7)
            ax.axhline(y=mean_diff, color='k', linestyle='-', label=f'Mean: {mean_diff:.2f}')
            ax.axhline(y=upper_loa, color='r', linestyle='--', 
                       label=f'Upper LoA: {upper_loa:.2f}')
            ax.axhline(y=lower_loa, color='r', linestyle='--', 
                       label=f'Lower LoA: {lower_loa:.2f}')
            
            ax.set_xlabel(f'Mean of {method1} and {method2} (ng/m³)')
            ax.set_ylabel(f'{method2} - {method1} (ng/m³)')
            ax.set_title(f'{method2} vs {method1}')
            ax.legend(fontsize='small')
            ax.grid(True, alpha=0.3)
        
        # Hide unused subplots
        for i in range(n_pairs, rows*cols):
            row, col = divmod(i, cols)
            axes[row, col].set_visible(False)
        
        plt.suptitle(f'Bland-Altman Plots - Daily Average {wavelength} BC', fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.94)
        plt.show()

# Create Bland-Altman plots
print("Creating Bland-Altman plots for method agreement analysis...")
create_bland_altman_plots(daily_dfs)


# %% [markdown]
# ### 20.6 Source Apportionment Daily Average Analysis

# %%
def analyze_sa_daily_averages(data_raw_sa, data_ona_sa, data_cma_sa, data_dema_sa):
    """
    Analyze source apportionment results on daily averages
    
    Parameters:
    -----------
    data_raw_sa : pandas.DataFrame
        DataFrame with raw source apportionment data
    data_ona_sa : pandas.DataFrame
        DataFrame with ONA source apportionment data
    data_cma_sa : pandas.DataFrame
        DataFrame with CMA source apportionment data
    data_dema_sa : pandas.DataFrame
        DataFrame with DEMA source apportionment data
    """
    print("\n===== Source Apportionment Daily Average Analysis =====")
    
    # Create dictionary of dataframes
    sa_dfs = {
        'Raw': data_raw_sa,
        'ONA': data_ona_sa,
        'CMA': data_cma_sa,
        'DEMA': data_dema_sa
    }
    
    # Check which dataframes have BB_Percent
    valid_sa_dfs = {}
    for name, df in sa_dfs.items():
        if 'BB_Percent' in df.columns:
            valid_sa_dfs[name] = df
    
    if not valid_sa_dfs:
        print("No source apportionment data available")
        return
    
    # Create combined dataframe for BB_Percent
    bb_percent_data = {}
    for name, df in valid_sa_dfs.items():
        bb_percent_data[name] = df['BB_Percent']
    
    bb_df = pd.DataFrame(bb_percent_data)
    bb_df['Time (UTC)'] = pd.to_datetime(data_raw_sa['Time (UTC)'])
    bb_df.set_index('Time (UTC)', inplace=True)
    
    # Calculate daily averages
    bb_daily = bb_df.resample('D').mean()
    
    # Plot time series
    plt.figure(figsize=(14, 8))
    
    for column in bb_daily.columns:
        if column == 'Raw':
            plt.plot(bb_daily.index, bb_daily[column], 'k-', alpha=0.6, label=column)
        elif column == 'ONA':
            plt.plot(bb_daily.index, bb_daily[column], 'b-', label=column)
        elif column == 'CMA':
            plt.plot(bb_daily.index, bb_daily[column], 'r-', label=column)
        elif column == 'DEMA':
            plt.plot(bb_daily.index, bb_daily[column], 'g-', label=column)
    
    plt.xlabel('Date')
    plt.ylabel('Biomass Burning (%)')
    plt.title('Daily Average Biomass Burning Percentage by Processing Method')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gcf().autofmt_xdate()
    
    plt.tight_layout()
    plt.show()
    
    # Create box plot
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=bb_daily)
    
    plt.ylabel('Biomass Burning (%)')
    plt.title('Distribution of Daily Average Biomass Burning % by Method')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    # Calculate statistics
    stats_df = bb_daily.agg(['count', 'mean', 'std', 'min', 'max', 'median']).T
    stats_df['cv'] = stats_df['std'] / stats_df['mean'] * 100  # Coefficient of variation
    
    print("\nStatistics for daily average biomass burning percentage by method:")
    display(stats_df.style.format({
        'count': '{:.0f}',
        'mean': '{:.2f}',
        'std': '{:.2f}',
        'min': '{:.2f}',
        'max': '{:.2f}',
        'median': '{:.2f}',
        'cv': '{:.2f}%'
    }))
    
    # Analyze paired differences
    print("\nPaired differences analysis for biomass burning percentage:")
    method_pairs = []
    for i, method1 in enumerate(bb_daily.columns):
        for method2 in bb_daily.columns[i+1:]:
            method_pairs.append((method1, method2))
    
    pair_results = []
    
    for method1, method2 in method_pairs:
        # Drop NaN values
        pair_df = bb_daily[[method1, method2]].dropna()
        
        if len(pair_df) < 2:
            print(f"Not enough data for {method1} vs {method2} comparison")
            continue
            
        # Calculate differences
        diff = pair_df[method2] - pair_df[method1]
        
        # Perform paired t-test
        t_stat, p_value = stats.ttest_rel(pair_df[method1], pair_df[method2])
        
        # Calculate correlation
        corr = pair_df[method1].corr(pair_df[method2])
        
        result = {
            'Method 1': method1,
            'Method 2': method2,
            'Mean Diff (%)': diff.mean(),
            'Median Diff (%)': diff.median(),
            'T-statistic': t_stat,
            'P-value': p_value,
            'Correlation': corr
        }
        
        pair_results.append(result)
    
    if pair_results:
        pair_results_df = pd.DataFrame(pair_results)
        display(pair_results_df.style.format({
            'Mean Diff (%)': '{:.2f}',
            'Median Diff (%)': '{:.2f}',
            'T-statistic': '{:.3f}',
            'P-value': '{:.4f}',
            'Correlation': '{:.3f}'
        }))
        
    return bb_daily

# Analyze source apportionment daily averages
print("Analyzing source apportionment daily averages...")
bb_daily = analyze_sa_daily_averages(
    processed_data_raw_sa, processed_data_ona_sa, 
    processed_data_cma_sa, processed_data_dema_sa
)

# %% [markdown]
# ### 20.7 Day of Week Analysis

# %%
def analyze_day_of_week_patterns(daily_dfs, bb_daily=None):
    """
    Analyze day-of-week patterns in daily averages
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
    bb_daily : pandas.DataFrame, optional
        DataFrame with daily biomass burning percentages
    """
    print("\n===== Day of Week Pattern Analysis =====")
    
    for wavelength, df in daily_dfs.items():
        # Add day of week column
        df_dow = df.copy()
        df_dow['Day of Week'] = df_dow.index.day_name()
        
        # Create day of week plots for each method
        for method in df.columns:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='Day of Week', y=method, data=df_dow.reset_index(), 
                       order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            
            plt.title(f'{wavelength} BC - {method} Method by Day of Week')
            plt.ylabel(f'{wavelength} BC (ng/m³)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()
            
            # Calculate statistics by day of week
            day_stats = df_dow.groupby('Day of Week')[method].agg(['mean', 'std', 'count'])
            day_stats = day_stats.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            day_stats['cv'] = day_stats['std'] / day_stats['mean'] * 100
            
            print(f"\nStatistics for {wavelength} BC - {method} Method by Day of Week:")
            display(day_stats.style.format({
                'mean': '{:.2f}',
                'std': '{:.2f}',
                'count': '{:.0f}',
                'cv': '{:.2f}%'
            }))
            
            # One-way ANOVA test for day of week effect
            groups = [df_dow[df_dow['Day of Week']==day][method].dropna().values 
                     for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
            groups = [g for g in groups if len(g) > 0]  # Remove empty groups
            
            if len(groups) > 1:
                f_stat, p_value = stats.f_oneway(*groups)
                print(f"One-way ANOVA for day of week effect on {wavelength} BC - {method} Method:")
                print(f"F-statistic: {f_stat:.3f}, p-value: {p_value:.4f}")
                if p_value < 0.05:
                    print("There is a statistically significant difference between days of the week.")
                else:
                    print("No statistically significant difference between days of the week.")
            else:
                print(f"Not enough data for ANOVA test for {wavelength} BC - {method} Method")
    
    # Analyze day of week patterns for biomass burning percentage
    if bb_daily is not None and not bb_daily.empty:
        bb_daily_dow = bb_daily.copy()
        bb_daily_dow['Day of Week'] = bb_daily_dow.index.day_name()
        
        for method in bb_daily.columns:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='Day of Week', y=method, data=bb_daily_dow.reset_index(), 
                       order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            
            plt.title(f'Biomass Burning % - {method} Method by Day of Week')
            plt.ylabel('Biomass Burning (%)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()
            
            # Calculate statistics by day of week
            day_stats = bb_daily_dow.groupby('Day of Week')[method].agg(['mean', 'std', 'count'])
            day_stats = day_stats.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            day_stats['cv'] = day_stats['std'] / day_stats['mean'] * 100
            
            print(f"\nStatistics for Biomass Burning % - {method} Method by Day of Week:")
            display(day_stats.style.format({
                'mean': '{:.2f}',
                'std': '{:.2f}',
                'count': '{:.0f}',
                'cv': '{:.2f}%'
            }))

# Analyze day of week patterns
print("Analyzing day of week patterns...")
analyze_day_of_week_patterns(daily_dfs, bb_daily)

# %% [markdown]
# ### 20.8 Seasonal Analysis of Daily Averages

# %%
def analyze_seasonal_patterns(daily_dfs, bb_daily=None):
    """
    Analyze seasonal patterns in daily averages
    
    Parameters:
    -----------
    daily_dfs : dict of pandas.DataFrame
        Dictionary with wavelength as key and daily averages DataFrame as value
    bb_daily : pandas.DataFrame, optional
        DataFrame with daily biomass burning percentages
    """
    print("\n===== Seasonal Pattern Analysis =====")
    
    for wavelength, df in daily_dfs.items():
        # Add season column
        df_season = df.copy()
        # Define seasons (Northern Hemisphere)
        df_season['Month'] = df_season.index.month
        df_season['Season'] = 'Unknown'
        df_season.loc[df_season['Month'].isin([12, 1, 2]), 'Season'] = 'Winter'
        df_season.loc[df_season['Month'].isin([3, 4, 5]), 'Season'] = 'Spring'
        df_season.loc[df_season['Month'].isin([6, 7, 8]), 'Season'] = 'Summer'
        df_season.loc[df_season['Month'].isin([9, 10, 11]), 'Season'] = 'Fall'
        
        # Create monthly trends for visualization
        df_month = df.copy()
        df_month['Month'] = df_month.index.month
        
        for method in df.columns:
            # Seasonal box plots
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='Season', y=method, data=df_season, 
                       order=['Winter', 'Spring', 'Summer', 'Fall'])
            
            plt.title(f'{wavelength} BC - {method} Method by Season')
            plt.ylabel(f'{wavelength} BC (ng/m³)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()
            
            # Monthly trends
            monthly_means = df_month.groupby('Month')[method].mean()
            monthly_std = df_month.groupby('Month')[method].std()
            
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_means.index, monthly_means.values, marker='o', linestyle='-', linewidth=2)
            plt.fill_between(monthly_means.index, 
                             monthly_means.values - monthly_std.values,
                             monthly_means.values + monthly_std.values, 
                             alpha=0.2)
            
            plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
            plt.xlabel('Month')
            plt.ylabel(f'{wavelength} BC (ng/m³)')
            plt.title(f'Monthly {wavelength} BC Trend - {method} Method')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()
            
            # Calculate statistics by season
            season_stats = df_season.groupby('Season')[method].agg(['mean', 'std', 'count'])
            season_stats = season_stats.reindex(['Winter', 'Spring', 'Summer', 'Fall'])
            season_stats['cv'] = season_stats['std'] / season_stats['mean'] * 100
            
            print(f"\nStatistics for {wavelength} BC - {method} Method by Season:")
            display(season_stats.style.format({
                'mean': '{:.2f}',
                'std': '{:.2f}',
                'count': '{:.0f}',
                'cv': '{:.2f}%'
            }))
            
            # One-way ANOVA test for seasonal effect
            groups = [df_season[df_season['Season']==season][method].dropna().values 
                     for season in ['Winter', 'Spring', 'Summer', 'Fall']]
            groups = [g for g in groups if len(g) > 0]  # Remove empty groups
            
            if len(groups) > 1:
                f_stat, p_value = stats.f_oneway(*groups)
                print(f"One-way ANOVA for seasonal effect on {wavelength} BC - {method} Method:")
                print(f"F-statistic: {f_stat:.3f}, p-value: {p_value:.4f}")
                if p_value < 0.05:
                    print("There is a statistically significant difference between seasons.")
                else:
                    print("No statistically significant difference between seasons.")
            else:
                print(f"Not enough data for ANOVA test for {wavelength} BC - {method} Method")
    
    # Analyze seasonal patterns for biomass burning percentage
    if bb_daily is not None and not bb_daily.empty:
        bb_daily_season = bb_daily.copy()
        bb_daily_season['Month'] = bb_daily_season.index.month
        bb_daily_season['Season'] = 'Unknown'
        bb_daily_season.loc[bb_daily_season['Month'].isin([12, 1, 2]), 'Season'] = 'Winter'
        bb_daily_season.loc[bb_daily_season['Month'].isin([3, 4, 5]), 'Season'] = 'Spring'
        bb_daily_season.loc[bb_daily_season['Month'].isin([6, 7, 8]), 'Season'] = 'Summer'
        bb_daily_season.loc[bb_daily_season['Month'].isin([9, 10, 11]), 'Season'] = 'Fall'
        
        # Create monthly data
        bb_daily_month = bb_daily.copy()
        bb_daily_month['Month'] = bb_daily_month.index.month
        
        for method in bb_daily.columns:
            # Seasonal box plots
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='Season', y=method, data=bb_daily_season, 
                       order=['Winter', 'Spring', 'Summer', 'Fall'])
            
            plt.title(f'Biomass Burning % - {method} Method by Season')
            plt.ylabel('Biomass Burning (%)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()
            
            # Monthly trends
            monthly_means = bb_daily_month.groupby('Month')[method].mean()
            monthly_std = bb_daily_month.groupby('Month')[method].std()
            
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_means.index, monthly_means.values, marker='o', linestyle='-', linewidth=2)
            plt.fill_between(monthly_means.index, 
                             monthly_means.values - monthly_std.values,
                             monthly_means.values + monthly_std.values, 
                             alpha=0.2)
            
            plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
            plt.xlabel('Month')
            plt.ylabel('Biomass Burning (%)')
            plt.title(f'Monthly Biomass Burning % Trend - {method} Method')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()

# Analyze seasonal patterns
print("Analyzing seasonal patterns...")
analyze_seasonal_patterns(daily_dfs, bb_daily)


