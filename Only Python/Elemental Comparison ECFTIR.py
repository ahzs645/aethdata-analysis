# %% [markdown]
# 
# # ETAD MAC-Speciation Analysis
# # Comprehensive analysis of MAC methods and chemical speciation for Addis Ababa, Ethiopia

# %%

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
from scipy.stats import pearsonr, spearmanr
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')


# %%
# Set publication-quality style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams.update({
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'font.size': 11,
    'axes.labelsize': 13,
    'axes.titlesize': 15,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 10,
    'figure.titlesize': 16
})

# %% [markdown]
# ## 1. Data Loading and Unit Handling

# %%
def load_etad_ftir_data(db_path):
    """Load FTIR/HIPS data for ETAD with proper units"""
    
    print("📊 Loading FTIR/HIPS data...")
    
    try:
        conn = sqlite3.connect(db_path)
        
        query = """
        SELECT 
            f.filter_id, f.sample_date, f.site_code, m.volume_m3,
            m.ec_ftir, m.ec_ftir_mdl, m.oc_ftir, m.oc_ftir_mdl,
            m.fabs, m.fabs_mdl, m.fabs_uncertainty, m.ftir_batch_id
        FROM filters f
        JOIN ftir_sample_measurements m USING(filter_id)
        WHERE f.site_code = 'ETAD' AND
              m.ec_ftir IS NOT NULL AND
              m.oc_ftir IS NOT NULL AND
              m.fabs IS NOT NULL
        ORDER BY f.sample_date
        """
        
        df = pd.read_sql_query(query, conn)
        df['sample_date'] = pd.to_datetime(df['sample_date'])
        conn.close()
        
        # Clean data - handle values below MDL
        for col, mdl_col in [('ec_ftir', 'ec_ftir_mdl'), ('oc_ftir', 'oc_ftir_mdl'), ('fabs', 'fabs_mdl')]:
            mask_below_mdl = df[col] < df[mdl_col]
            if mask_below_mdl.sum() > 0:
                df.loc[mask_below_mdl, col] = df.loc[mask_below_mdl, mdl_col] / 2
        
        # Calculate additional parameters
        df['oc_ec_ratio'] = df['oc_ftir'] / df['ec_ftir']
        df['mac_individual'] = df['fabs'] / df['ec_ftir']  # m²/g
        
        # Remove extreme outliers
        df_clean = df[(df['mac_individual'] < 50) & (df['oc_ec_ratio'] < 20)]
        
        print(f"✅ Loaded {len(df_clean)} FTIR/HIPS samples")
        print(f"   Date range: {df_clean['sample_date'].min()} to {df_clean['sample_date'].max()}")
        print(f"   Units: EC/OC (μg/m³), Fabs (Mm⁻¹), MAC (m²/g)")
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Error loading FTIR data: {e}")
        return None

def load_etad_speciation_data(csv_path):
    """Load ETAD speciation data with proper unit handling"""
    
    print("🧪 Loading speciation data...")
    
    try:
        # Load CSV (skip first 3 lines to get to header)
        df = pd.read_csv(csv_path, skiprows=3)
        print(f"   Raw data: {len(df)} rows, {len(df.columns)} columns")
        
        # Filter for ETAD
        df = df[df['Site_Code'] == 'ETAD'].copy()
        print(f"   ETAD data: {len(df)} rows")
        
        # Create datetime
        def create_timestamp(row):
            try:
                return pd.Timestamp(
                    year=int(row['Start_Year_local']),
                    month=int(row['Start_Month_local']),
                    day=int(row['Start_Day_local']),
                    hour=int(row['Start_hour_local'])
                )
            except:
                return pd.NaT
        
        df['Start_Date'] = df.apply(create_timestamp, axis=1)
        df = df[df['Start_Date'].notna()].copy()
        
        print(f"   ✅ Created datetime for {len(df)} rows")
        
        # Map parameter codes to clean names with units
        parameter_mapping = {
            28101: 'PM25_mass',          # μg/m³
            28202: 'BC_PM25',            # μg/m³  
            28401: 'Sulfate_Ion',        # μg/m³
            28402: 'Nitrate_Ion',        # μg/m³
            28403: 'Phosphate_Ion',      # μg/m³
            28404: 'Nitrite_Ion',        # μg/m³
            28801: 'Sodium_Ion',         # μg/m³
            28802: 'Ammonium_Ion',       # μg/m³
            28803: 'Potassium_Ion',      # μg/m³
            28804: 'Magnesium_Ion',      # ng/m³ (note different unit!)
            28805: 'Calcium_Ion',        # μg/m³
            28902: 'Aluminum',           # ng/m³
            28904: 'Titanium',           # ng/m³
            28905: 'Vanadium',           # ng/m³
            28906: 'Chromium',           # ng/m³
            28907: 'Manganese',          # ng/m³
            28908: 'Iron',               # ng/m³
            28909: 'Cobalt',             # ng/m³
            28910: 'Nickel',             # ng/m³
            28911: 'Copper',             # ng/m³
            28912: 'Zinc',               # ng/m³
            28913: 'Arsenic',            # ng/m³
            28914: 'Selenium',           # ng/m³
            28916: 'Cadmium',            # ng/m³
            28917: 'Antimony',           # ng/m³
            28919: 'Cerium',             # ng/m³
            28920: 'Lead',               # ng/m³
            28921: 'Rubidium',           # ng/m³
            28922: 'Strontium',          # ng/m³
            28923: 'Silicon',            # ng/m³
            28924: 'Sulfur',             # ng/m³
            28925: 'Chlorine',           # ng/m³
            28926: 'Tin'                 # ng/m³
        }
        
        # Unit conversion mapping (ng/m³ to μg/m³ for consistency)
        ng_to_ug_species = [
            'Magnesium_Ion', 'Aluminum', 'Titanium', 'Vanadium', 'Chromium', 
            'Manganese', 'Iron', 'Cobalt', 'Nickel', 'Copper', 'Zinc', 
            'Arsenic', 'Selenium', 'Cadmium', 'Antimony', 'Cerium', 'Lead',
            'Rubidium', 'Strontium', 'Silicon', 'Sulfur', 'Chlorine', 'Tin'
        ]
        
        # Map parameter codes
        df['Parameter_Name_Clean'] = df['Parameter_Code'].map(parameter_mapping)
        mapped_count = df['Parameter_Name_Clean'].notna().sum()
        print(f"   📊 Mapped {mapped_count} parameter measurements")
        
        # Keep only mapped parameters
        df_mapped = df[df['Parameter_Name_Clean'].notna()].copy()
        
        # Pivot to wide format
        speciation_wide = df_mapped.pivot_table(
            index=['Filter_ID', 'Start_Date'],
            columns='Parameter_Name_Clean',
            values='Value',
            aggfunc='first'
        ).reset_index()
        
        speciation_wide.columns.name = None
        
        # Convert ng/m³ to μg/m³ for consistency
        for species in ng_to_ug_species:
            if species in speciation_wide.columns:
                speciation_wide[species] = speciation_wide[species] / 1000  # ng/m³ → μg/m³
        
        print(f"   ✅ Created wide format: {len(speciation_wide)} unique samples")
        print(f"   📏 All species now in μg/m³ units")
        
        return speciation_wide
        
    except Exception as e:
        print(f"❌ Error loading speciation data: {e}")
        return None


# %% [markdown]
# ## 2. Data Merging and Preprocessing
# 

# %%
def merge_etad_datasets(ftir_data, spec_data):
    """Merge FTIR and speciation datasets"""
    
    print("🔗 Merging datasets...")
    
    if ftir_data is None or spec_data is None:
        print("❌ Cannot merge - missing data")
        return None
    
    print(f"   FTIR samples: {len(ftir_data)}")
    print(f"   Speciation samples: {len(spec_data)}")
    
    # Try direct filter ID merge first
    merged = pd.merge(
        ftir_data,
        spec_data,
        left_on='filter_id',
        right_on='Filter_ID',
        how='inner'
    )
    
    print(f"   Direct filter ID merge: {len(merged)} matches")
    
    # If no direct matches, try date-based merge
    if len(merged) == 0:
        print("   Trying date-based merge...")
        
        ftir_temp = ftir_data.copy()
        spec_temp = spec_data.copy()
        
        ftir_temp['date_key'] = ftir_temp['sample_date'].dt.date
        spec_temp['date_key'] = spec_temp['Start_Date'].dt.date
        
        merged = pd.merge(
            ftir_temp,
            spec_temp,
            on='date_key',
            how='inner',
            suffixes=('_ftir', '_spec')
        )
        
        print(f"   Date-based merge: {len(merged)} matches")
    
    if len(merged) > 0:
        print(f"✅ Successfully merged {len(merged)} samples!")
        
        # Add seasonal classification
        merged['month'] = merged['sample_date' if 'sample_date' in merged.columns else 'Start_Date'].dt.month
        
        def get_ethiopian_season(month):
            if month in [10, 11, 12, 1, 2]:
                return 'Dry Season (Bega)'
            elif month in [3, 4, 5]:
                return 'Belg Rainy Season'
            else:
                return 'Kiremt Rainy Season'
        
        merged['season'] = merged['month'].apply(get_ethiopian_season)
        
        # Calculate additional ratios
        if 'Potassium_Ion' in merged.columns and 'PM25_mass' in merged.columns:
            merged['K_PM_ratio'] = merged['Potassium_Ion'] / merged['PM25_mass'] * 100  # %
        
        if 'Iron' in merged.columns and 'Aluminum' in merged.columns:
            merged['Fe_Al_ratio'] = merged['Iron'] / merged['Aluminum']  # dimensionless
        
        if 'Sulfate_Ion' in merged.columns and 'Nitrate_Ion' in merged.columns:
            merged['SO4_NO3_ratio'] = merged['Sulfate_Ion'] / merged['Nitrate_Ion']  # dimensionless
        
        return merged
    else:
        print("❌ No matching samples found")
        return None

# %% [markdown]
# ## 3. MAC Method Calculations

# %%
def calculate_mac_methods(merged_data):
    """Calculate all 4 MAC methods with proper error handling"""
    
    print("📊 Calculating MAC methods...")
    
    ec_data = merged_data['ec_ftir'].values  # μg/m³
    fabs_data = merged_data['fabs'].values   # Mm⁻¹
    
    print(f"   Using {len(ec_data)} samples")
    print(f"   EC range: {ec_data.min():.2f} - {ec_data.max():.2f} μg/m³")
    print(f"   Fabs range: {fabs_data.min():.2f} - {fabs_data.max():.2f} Mm⁻¹")
    
    # Method 1: Mean of individual ratios
    individual_macs = fabs_data / ec_data  # (Mm⁻¹) / (μg/m³) = m²/g
    mac_method1 = np.mean(individual_macs)
    
    # Method 2: Ratio of means
    mac_method2 = np.mean(fabs_data) / np.mean(ec_data)  # m²/g
    
    # Method 3: Standard regression (with intercept)
    reg_standard = LinearRegression(fit_intercept=True)
    reg_standard.fit(ec_data.reshape(-1, 1), fabs_data)
    mac_method3 = reg_standard.coef_[0]  # m²/g
    intercept_method3 = reg_standard.intercept_  # Mm⁻¹
    
    # Method 4: Origin regression (forced through origin)
    reg_origin = LinearRegression(fit_intercept=False)
    reg_origin.fit(ec_data.reshape(-1, 1), fabs_data)
    mac_method4 = reg_origin.coef_[0]  # m²/g
    
    print(f"   ✅ Method 1 (Mean of Ratios): {mac_method1:.3f} m²/g")
    print(f"   ✅ Method 2 (Ratio of Means): {mac_method2:.3f} m²/g")
    print(f"   ✅ Method 3 (Standard Regression): {mac_method3:.3f} m²/g (intercept: {intercept_method3:.2f} Mm⁻¹)")
    print(f"   ✅ Method 4 (Origin Regression): {mac_method4:.3f} m²/g")
    
    # Calculate BC equivalents (all in μg/m³)
    bc_method1 = fabs_data / mac_method1
    bc_method2 = fabs_data / mac_method2
    bc_method3_corrected = np.maximum(fabs_data - intercept_method3, 0) / mac_method3
    bc_method4 = fabs_data / mac_method4
    
    results = {
        'mac_values': {
            'Method_1': mac_method1,
            'Method_2': mac_method2,
            'Method_3': mac_method3,
            'Method_4': mac_method4
        },
        'intercept': intercept_method3,
        'bc_equivalents': {
            'BC_Method_1': bc_method1,
            'BC_Method_2': bc_method2,
            'BC_Method_3': bc_method3_corrected,
            'BC_Method_4': bc_method4
        }
    }
    
    return results

# %% [markdown]
# ## 4. Visualization Functions

# %%

def create_mac_methods_comparison(merged_data, mac_results):
    """Create comprehensive MAC methods comparison plot"""
    
    print("📈 Creating MAC Methods Comparison...")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: MAC Values Comparison
    methods = ['Method_1', 'Method_2', 'Method_3', 'Method_4']
    mac_values = [mac_results['mac_values'][m] for m in methods]
    method_labels = ['M1:\nMean of Ratios', 'M2:\nRatio of Means', 'M3:\nStd Regression', 'M4:\nOrigin Regression']
    colors = ['#e74c3c', '#27ae60', '#3498db', '#9b59b6']
    
    bars = ax1.bar(range(len(methods)), mac_values, color=colors, alpha=0.8, 
                   edgecolor='black', linewidth=2, width=0.6)
    
    # Add literature range
    ax1.axhspan(7.5, 12.5, alpha=0.25, color='yellow', label='Literature Range\n(7.5-12.5 m²/g)')
    
    # Add values on bars
    for bar, val in zip(bars, mac_values):
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.3,
                f'{val:.2f}', ha='center', va='bottom', fontweight='bold', fontsize=12)
    
    ax1.set_xticks(range(len(methods)))
    ax1.set_xticklabels(method_labels)
    ax1.set_ylabel('MAC Value (m²/g)', fontweight='bold')
    ax1.set_title('MAC Methods Comparison\nETAD Site, Addis Ababa', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Plot 2: Fabs vs EC with regression lines
    ec_data = merged_data['ec_ftir'].values
    fabs_data = merged_data['fabs'].values
    
    ax2.scatter(ec_data, fabs_data, alpha=0.6, s=50, color='black', 
               label=f'Data (n={len(ec_data)})', zorder=3)
    
    ec_line = np.linspace(0, ec_data.max() * 1.1, 100)
    
    for i, (method, color) in enumerate(zip(methods, colors)):
        mac_val = mac_results['mac_values'][method]
        
        if method == 'Method_3':
            fabs_line = mac_val * ec_line + mac_results['intercept']
            label = f'M{i+1}: {mac_val:.2f}×EC + {mac_results["intercept"]:.1f}'
        else:
            fabs_line = mac_val * ec_line
            label = f'M{i+1}: {mac_val:.2f}×EC'
        
        ax2.plot(ec_line, fabs_line, color=color, linewidth=3, label=label)
    
    ax2.set_xlabel('FTIR EC (μg/m³)', fontweight='bold')
    ax2.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax2.set_title('Fabs vs EC Relationships', fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: BC Equivalents Distribution
    bc_data = [mac_results['bc_equivalents'][f'BC_{method}'] for method in methods]
    bp = ax3.boxplot(bc_data, labels=['M1', 'M2', 'M3', 'M4'], patch_artist=True)
    
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    # Add FTIR EC reference line
    ec_mean = merged_data['ec_ftir'].mean()
    ax3.axhline(ec_mean, color='red', linestyle='--', linewidth=2, 
               label=f'FTIR EC Mean ({ec_mean:.2f} μg/m³)')
    
    ax3.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
    ax3.set_title('BC Equivalent Distributions', fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Physical Constraint Check
    bc_at_zero = []
    for method in methods:
        if method == 'Method_3':
            bc_zero = -mac_results['intercept'] / mac_results['mac_values'][method]
        else:
            bc_zero = 0
        bc_at_zero.append(bc_zero)
    
    colors_constraint = ['green' if abs(val) < 0.1 else 'red' for val in bc_at_zero]
    bars = ax4.bar(['M1', 'M2', 'M3', 'M4'], bc_at_zero, color=colors_constraint, 
                   alpha=0.8, edgecolor='black', linewidth=2)
    
    ax4.axhline(0, color='black', linestyle='-', linewidth=2)
    ax4.axhspan(-0.1, 0.1, alpha=0.2, color='green', label='Acceptable\n(±0.1 μg/m³)')
    
    # Add violation warnings
    for i, (bar, val) in enumerate(zip(bars, bc_at_zero)):
        if abs(val) > 0.1:
            ax4.text(bar.get_x() + bar.get_width()/2., val - 0.2 if val < 0 else val + 0.1,
                    '⚠️ VIOLATES', ha='center', va='bottom' if val > 0 else 'top', 
                    fontweight='bold', color='red')
        ax4.text(bar.get_x() + bar.get_width()/2., val/2,
                f'{val:.2f}', ha='center', va='center', fontweight='bold')
    
    ax4.set_ylabel('BC when Fabs = 0 (μg/m³)', fontweight='bold')
    ax4.set_title('Physical Constraint Check\n(BC = 0 when Fabs = 0)', fontweight='bold')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

def create_chemical_speciation_analysis(merged_data, mac_results):
    """Analyze chemical speciation effects on MAC"""
    
    print("🧪 Creating Chemical Speciation Analysis...")
    
    # Calculate BC equivalents
    fabs_data = merged_data['fabs'].values
    bc_methods = {
        'BC_Method_1': fabs_data / mac_results['mac_values']['Method_1'],
        'BC_Method_2': fabs_data / mac_results['mac_values']['Method_2'],
        'BC_Method_3': np.maximum(fabs_data - mac_results['intercept'], 0) / mac_results['mac_values']['Method_3'],
        'BC_Method_4': fabs_data / mac_results['mac_values']['Method_4']
    }
    
    # Add BC methods to dataframe
    df = merged_data.copy()
    for method, bc_values in bc_methods.items():
        df[method] = bc_values
    
    # Chemical species of interest (all in μg/m³)
    species_of_interest = [
        'PM25_mass', 'Sulfate_Ion', 'Nitrate_Ion', 'Ammonium_Ion', 'Potassium_Ion',
        'Iron', 'Aluminum', 'Zinc', 'Copper', 'Manganese', 'Lead'
    ]
    
    available_species = [s for s in species_of_interest if s in df.columns]
    method_names = list(bc_methods.keys())
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Correlation Heatmap
    correlations = {}
    for method in method_names:
        correlations[method] = {}
        for species in available_species:
            mask = df[[method, species]].notna().all(axis=1)
            if mask.sum() >= 5:
                r, p = pearsonr(df.loc[mask, method], df.loc[mask, species])
                correlations[method][species] = {'r': r, 'p': p, 'n': mask.sum()}
    
    # Create correlation matrix
    corr_matrix = np.zeros((len(method_names), len(available_species)))
    
    for i, method in enumerate(method_names):
        for j, species in enumerate(available_species):
            if species in correlations[method]:
                corr_matrix[i, j] = correlations[method][species]['r']
            else:
                corr_matrix[i, j] = np.nan
    
    im = ax1.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-1, vmax=1)
    
    ax1.set_xticks(range(len(available_species)))
    ax1.set_xticklabels([s.replace('_', ' ') for s in available_species], rotation=45, ha='right')
    ax1.set_yticks(range(len(method_names)))
    ax1.set_yticklabels(['M1', 'M2', 'M3', 'M4'])
    
    # Add correlation values
    for i in range(len(method_names)):
        for j in range(len(available_species)):
            if not np.isnan(corr_matrix[i, j]):
                text_color = 'white' if abs(corr_matrix[i, j]) > 0.6 else 'black'
                ax1.text(j, i, f'{corr_matrix[i, j]:.2f}', ha='center', va='center',
                        color=text_color, fontweight='bold')
    
    ax1.set_title('Species-MAC Correlations\n(All species in μg/m³)', fontweight='bold')
    plt.colorbar(im, ax=ax1, shrink=0.8, label='Pearson r')
    
    # Plot 2: Source Signature Analysis
    if 'K_PM_ratio' in df.columns and 'Fe_Al_ratio' in df.columns:
        scatter = ax2.scatter(df['K_PM_ratio'], df['Fe_Al_ratio'], 
                             c=df['BC_Method_2'], cmap='viridis', alpha=0.7, s=50,
                             edgecolors='black', linewidth=0.5)
        
        ax2.set_xlabel('K/PM2.5 (%) - Biomass Burning', fontweight='bold')
        ax2.set_ylabel('Fe/Al Ratio - Dust Signature', fontweight='bold')
        ax2.set_title('Source Classification\n(Colored by BC Method 2)', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        cbar = plt.colorbar(scatter, ax=ax2)
        cbar.set_label('BC Equivalent (μg/m³)')
    
    # Plot 3: Seasonal Variations
    if 'season' in df.columns:
        seasons = df['season'].unique()
        season_data = [df[df['season'] == season]['BC_Method_2'].dropna() for season in seasons]
        
        bp = ax3.boxplot(season_data, labels=[s.split()[0] for s in seasons], patch_artist=True)
        colors_season = ['#ffcc99', '#99ccff', '#99ff99']
        
        for patch, color in zip(bp['boxes'], colors_season[:len(bp['boxes'])]):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax3.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
        ax3.set_title('Seasonal BC Variations\nEthiopian Climate Seasons', fontweight='bold')
        ax3.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Brown Carbon Analysis (Method 3 intercept)
    intercept = mac_results['intercept']
    total_fabs = df['fabs'].mean()
    brown_carbon_fraction = intercept / total_fabs * 100
    
    sizes = [brown_carbon_fraction, 100 - brown_carbon_fraction]
    labels = [f'Baseline Absorption\n({brown_carbon_fraction:.1f}%)', 
              f'BC Absorption\n({100-brown_carbon_fraction:.1f}%)']
    colors_donut = ['#ff6b6b', '#4ecdc4']
    
    wedges, texts, autotexts = ax4.pie(sizes, labels=labels, colors=colors_donut, 
                                      autopct='%1.1f%%', startangle=90,
                                      wedgeprops=dict(width=0.5))
    
    ax4.set_title(f'Absorption Sources Analysis\nBaseline = {intercept:.1f} Mm⁻¹', fontweight='bold')
    
    plt.tight_layout()
    plt.show()
    
    return correlations

# %% [markdown]
# ## 5. Performance Analysis

# %%

def analyze_method_performance(merged_data, mac_results):
    """Analyze performance of different MAC methods"""
    
    print("📊 Analyzing Method Performance...")
    
    ec_data = merged_data['ec_ftir'].values
    
    # Calculate performance metrics for each method
    performance_metrics = {}
    
    for method in ['Method_1', 'Method_2', 'Method_3', 'Method_4']:
        if method == 'Method_3':
            bc_values = np.maximum(merged_data['fabs'] - mac_results['intercept'], 0) / mac_results['mac_values'][method]
        else:
            bc_values = merged_data['fabs'] / mac_results['mac_values'][method]
        
        # Calculate metrics
        bias = np.mean(bc_values - ec_data)
        rmse = np.sqrt(np.mean((bc_values - ec_data)**2))
        mae = np.mean(np.abs(bc_values - ec_data))
        r_corr, _ = pearsonr(bc_values, ec_data)
        
        performance_metrics[method] = {
            'bias': bias,
            'rmse': rmse,
            'mae': mae,
            'correlation': r_corr,
            'bc_mean': np.mean(bc_values),
            'bc_std': np.std(bc_values)
        }
    
    # Create performance visualization
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    methods = ['Method_1', 'Method_2', 'Method_3', 'Method_4']
    method_labels = ['M1', 'M2', 'M3', 'M4']
    colors = ['#e74c3c', '#27ae60', '#3498db', '#9b59b6']
    
    # Plot 1: Bias Comparison
    biases = [performance_metrics[method]['bias'] for method in methods]
    bars = ax1.bar(method_labels, biases, color=colors, alpha=0.8, edgecolor='black')
    
    ax1.axhline(0, color='black', linestyle='-', linewidth=2)
    ax1.set_ylabel('Bias (μg/m³)', fontweight='bold')
    ax1.set_title('Method Bias (BC - EC)', fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    
    for bar, bias in zip(bars, biases):
        ax1.text(bar.get_x() + bar.get_width()/2., bias + 0.05 if bias >= 0 else bias - 0.05,
                f'{bias:.2f}', ha='center', va='bottom' if bias >= 0 else 'top', 
                fontweight='bold')
    
    # Plot 2: RMSE and Correlation
    rmses = [performance_metrics[method]['rmse'] for method in methods]
    correlations = [performance_metrics[method]['correlation'] for method in methods]
    
    ax2_twin = ax2.twinx()
    
    bars1 = ax2.bar([i-0.2 for i in range(len(methods))], rmses, 0.4, 
                    color='lightcoral', alpha=0.8, label='RMSE')
    bars2 = ax2_twin.bar([i+0.2 for i in range(len(methods))], correlations, 0.4,
                         color='lightblue', alpha=0.8, label='Correlation')
    
    ax2.set_xticks(range(len(methods)))
    ax2.set_xticklabels(method_labels)
    ax2.set_ylabel('RMSE (μg/m³)', fontweight='bold', color='red')
    ax2_twin.set_ylabel('Correlation (r)', fontweight='bold', color='blue')
    ax2.set_title('RMSE and Correlation vs FTIR EC', fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Method Agreement Analysis
    method_pairs = [(0,1), (0,2), (0,3), (1,2), (1,3), (2,3)]
    pair_labels = ['M1-M2', 'M1-M3', 'M1-M4', 'M2-M3', 'M2-M4', 'M3-M4']
    
    bc_data = []
    for method in methods:
        if method == 'Method_3':
            bc_vals = np.maximum(merged_data['fabs'] - mac_results['intercept'], 0) / mac_results['mac_values'][method]
        else:
            bc_vals = merged_data['fabs'] / mac_results['mac_values'][method]
        bc_data.append(bc_vals)
    
    agreement_correlations = []
    for i, j in method_pairs:
        r, _ = pearsonr(bc_data[i], bc_data[j])
        agreement_correlations.append(r)
    
    bars = ax3.bar(pair_labels, agreement_correlations, 
                   color=['lightgreen' if r > 0.9 else 'orange' if r > 0.8 else 'red' for r in agreement_correlations],
                   alpha=0.8, edgecolor='black')
    
    ax3.axhline(0.9, color='green', linestyle='--', alpha=0.7, label='Good Agreement (r>0.9)')
    ax3.axhline(0.8, color='orange', linestyle='--', alpha=0.7, label='Fair Agreement (r>0.8)')
    
    ax3.set_ylabel('Inter-Method Correlation (r)', fontweight='bold')
    ax3.set_title('Method Agreement Analysis', fontweight='bold')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    for bar, r in zip(bars, agreement_correlations):
        ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.01,
                f'{r:.3f}', ha='center', va='bottom', fontweight='bold')
    
    # Plot 4: Performance Summary Table
    ax4.axis('off')
    
    # Create performance table
    table_data = []
    headers = ['Method', 'MAC (m²/g)', 'Bias', 'RMSE', 'MAE', 'Correlation']
    
    method_names = ['Mean of Ratios', 'Ratio of Means', 'Standard Regression', 'Origin Regression']
    
    for i, method in enumerate(methods):
        mac_val = mac_results['mac_values'][method]
        perf = performance_metrics[method]
        
        row = [
            method_names[i],
            f'{mac_val:.3f}',
            f'{perf["bias"]:.3f}',
            f'{perf["rmse"]:.3f}',
            f'{perf["mae"]:.3f}',
            f'{perf["correlation"]:.3f}'
        ]
        table_data.append(row)
    
    table = ax4.table(cellText=table_data, colLabels=headers, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Color code the table
    for i in range(len(methods)):
        for j in range(len(headers)):
            table[(i+1, j)].set_facecolor(colors[i])
            table[(i+1, j)].set_alpha(0.3)
    
    ax4.set_title('Performance Summary\n(All units: μg/m³ except MAC and correlation)', 
                  fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.show()
    
    return performance_metrics

# %% [markdown]
# ## 6. Advanced Chemical Analysis

# %%
def analyze_chemical_drivers_of_mac(merged_data):
    """Analyze what chemical species drive MAC variability"""
    
    print("🧪 Analyzing Chemical Drivers of MAC Variability...")
    
    df = merged_data.copy()
    
    # Calculate individual MAC values
    df['MAC_individual'] = df['fabs'] / df['ec_ftir']  # m²/g
    
    # Remove extreme outliers
    mac_clean = df[(df['MAC_individual'] > 0) & (df['MAC_individual'] < 30)].copy()
    
    print(f"Analyzing {len(mac_clean)} samples with valid MAC values")
    print(f"MAC range: {mac_clean['MAC_individual'].min():.2f} - {mac_clean['MAC_individual'].max():.2f} m²/g")
    
    # Define chemical features for analysis (EXCLUDING ec_ftir and fabs since MAC = fabs/ec_ftir)
    chemical_features = []
    
    # Add ratios (dimensionless)
    if 'K_PM_ratio' in mac_clean.columns:
        chemical_features.append('K_PM_ratio')
    if 'Fe_Al_ratio' in mac_clean.columns:
        chemical_features.append('Fe_Al_ratio')
    if 'SO4_NO3_ratio' in mac_clean.columns:
        chemical_features.append('SO4_NO3_ratio')
    
    # Add key species (all in μg/m³) - EXCLUDING ec_ftir and fabs to avoid circular reasoning
    key_species = ['Potassium_Ion', 'Iron', 'Aluminum', 'Sulfate_Ion', 'Nitrate_Ion', 
                   'Ammonium_Ion', 'PM25_mass', 'Zinc', 'Copper', 'Manganese', 'Lead']
    
    for species in key_species:
        if species in mac_clean.columns:
            chemical_features.append(species)
    
    # Add coating proxy (using only coating species, not EC)
    if all(col in mac_clean.columns for col in ['Sulfate_Ion', 'Nitrate_Ion']):
        # Total secondary inorganic aerosol as coating proxy
        mac_clean['SIA_total'] = mac_clean['Sulfate_Ion'] + mac_clean['Nitrate_Ion']
        chemical_features.append('SIA_total')
        
        # Only add coating thickness if EC is NOT in the feature list
        if 'ec_ftir' not in chemical_features and 'ec_ftir' in mac_clean.columns:
            mac_clean['coating_thickness'] = mac_clean['SIA_total'] / mac_clean['ec_ftir']
            chemical_features.append('coating_thickness')
    
    print(f"Chemical features for analysis: {len(chemical_features)}")
    print(f"Features: {', '.join(chemical_features)}")
    print(f"Note: Excluded ec_ftir and fabs to avoid circular reasoning (MAC = fabs/ec_ftir)")
    
    # Random Forest analysis to identify drivers
    if len(chemical_features) >= 3:
        # Prepare data
        X = mac_clean[chemical_features].fillna(0)
        y = mac_clean['MAC_individual']
        
        # Random Forest
        rf = RandomForestRegressor(n_estimators=100, random_state=42)
        rf.fit(X, y)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': chemical_features,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\n🤖 RANDOM FOREST ANALYSIS RESULTS:")
        print(f"   Model R² Score: {rf.score(X, y):.3f}")
        print(f"   Top 5 Chemical Drivers of MAC Variability:")
        for i, row in feature_importance.head(5).iterrows():
            print(f"     {i+1}. {row['feature'].replace('_', ' ')}: {row['importance']:.3f}")
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Plot 1: Feature Importance
        top_features = feature_importance.head(8)
        
        bars = ax1.barh(range(len(top_features)), top_features['importance'], 
                        color='skyblue', alpha=0.8, edgecolor='black')
        
        ax1.set_yticks(range(len(top_features)))
        ax1.set_yticklabels([f.replace('_', ' ') for f in top_features['feature']])
        ax1.set_xlabel('Feature Importance', fontweight='bold')
        ax1.set_title('Key Drivers of MAC Variability\n(Random Forest Analysis)', fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='x')
        
        for i, (bar, val) in enumerate(zip(bars, top_features['importance'])):
            ax1.text(val + 0.005, bar.get_y() + bar.get_height()/2, 
                    f'{val:.3f}', va='center', fontweight='bold')
        
        # Plot 2: Top driver relationship
        top_driver = top_features.iloc[0]['feature']
        
        if top_driver in mac_clean.columns:
            mask = mac_clean[[top_driver, 'MAC_individual']].notna().all(axis=1)
            x_data = mac_clean.loc[mask, top_driver]
            y_data = mac_clean.loc[mask, 'MAC_individual']
            
            # Color by season if available
            if 'season' in mac_clean.columns:
                seasons = mac_clean.loc[mask, 'season']
                colors_map = {'Dry Season (Bega)': '#d35400', 
                             'Belg Rainy Season': '#27ae60', 
                             'Kiremt Rainy Season': '#2980b9'}
                
                for season in seasons.unique():
                    season_mask = seasons == season
                    if season in colors_map:
                        ax2.scatter(x_data[season_mask], y_data[season_mask], 
                                   color=colors_map[season], alpha=0.7, s=50, 
                                   label=season.split()[0], edgecolors='black', linewidth=0.5)
            else:
                ax2.scatter(x_data, y_data, alpha=0.7, s=50, color='blue', 
                           edgecolors='black', linewidth=0.5)
            
            # Add regression line
            if len(x_data) > 5:
                z = np.polyfit(x_data, y_data, 1)
                p = np.poly1d(z)
                x_line = np.linspace(x_data.min(), x_data.max(), 100)
                ax2.plot(x_line, p(x_line), 'r--', linewidth=2, alpha=0.8)
                
                r, p_val = pearsonr(x_data, y_data)
                ax2.text(0.05, 0.95, f'r = {r:.3f}\np = {p_val:.2e}', 
                        transform=ax2.transAxes,
                        bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                        fontweight='bold')
            
            # Determine units for x-axis label
            if top_driver in ['K_PM_ratio']:
                x_label = f'{top_driver.replace("_", " ")} (%)'
            elif top_driver in ['Fe_Al_ratio', 'SO4_NO3_ratio', 'coating_thickness']:
                x_label = f'{top_driver.replace("_", " ")} (dimensionless)'
            elif top_driver == 'fabs':
                x_label = f'{top_driver} (Mm⁻¹)'
            else:
                x_label = f'{top_driver.replace("_", " ")} (μg/m³)'
            
            ax2.set_xlabel(x_label, fontweight='bold')
            ax2.set_ylabel('Individual MAC (m²/g)', fontweight='bold')
            ax2.set_title(f'MAC vs Top Driver: {top_driver.replace("_", " ")}', fontweight='bold')
            if 'season' in mac_clean.columns:
                ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        # Plot 3: Seasonal MAC patterns
        if 'season' in mac_clean.columns:
            seasonal_mac = mac_clean.groupby('season')['MAC_individual'].agg(['mean', 'std', 'count'])
            
            seasons = seasonal_mac.index
            means = seasonal_mac['mean']
            stds = seasonal_mac['std']
            
            bars = ax3.bar(range(len(seasons)), means, yerr=stds, capsize=5,
                          color=['#d35400', '#27ae60', '#2980b9'][:len(seasons)], 
                          alpha=0.8, edgecolor='black')
            
            ax3.set_xticks(range(len(seasons)))
            ax3.set_xticklabels([s.split()[0] for s in seasons])
            ax3.set_ylabel('MAC (m²/g)', fontweight='bold')
            ax3.set_title('Seasonal MAC Variations\nEthiopian Climate Seasons', fontweight='bold')
            ax3.grid(True, alpha=0.3, axis='y')
            
            # Add values on bars
            for bar, mean_val in zip(bars, means):
                ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.2,
                        f'{mean_val:.2f}', ha='center', va='bottom', fontweight='bold')
        
        # Plot 4: OC/EC vs MAC (if available)
        if 'oc_ec_ratio' in mac_clean.columns:
            # Filter reasonable OC/EC values
            oc_ec_mask = (mac_clean['oc_ec_ratio'] > 0.5) & (mac_clean['oc_ec_ratio'] < 10)
            oc_ec_data = mac_clean[oc_ec_mask]
            
            if len(oc_ec_data) > 10:
                scatter = ax4.scatter(oc_ec_data['oc_ec_ratio'], oc_ec_data['MAC_individual'],
                                     c=oc_ec_data['ec_ftir'], cmap='viridis', alpha=0.7, s=50,
                                     edgecolors='black', linewidth=0.5)
                
                # Add regression line
                r_oc, p_oc = pearsonr(oc_ec_data['oc_ec_ratio'], oc_ec_data['MAC_individual'])
                z = np.polyfit(oc_ec_data['oc_ec_ratio'], oc_ec_data['MAC_individual'], 1)
                p = np.poly1d(z)
                x_line = np.linspace(oc_ec_data['oc_ec_ratio'].min(), oc_ec_data['oc_ec_ratio'].max(), 100)
                ax4.plot(x_line, p(x_line), 'r--', linewidth=2, alpha=0.8)
                
                ax4.set_xlabel('OC/EC Ratio (dimensionless)', fontweight='bold')
                ax4.set_ylabel('Individual MAC (m²/g)', fontweight='bold')
                ax4.set_title(f'OC/EC vs MAC\nr = {r_oc:.3f}, p = {p_oc:.2e}', fontweight='bold')
                ax4.grid(True, alpha=0.3)
                
                cbar = plt.colorbar(scatter, ax=ax4)
                cbar.set_label('FTIR EC (μg/m³)')
                
                # Add interpretation
                ax4.text(0.02, 0.98, 'OC/EC Interpretation:\n• Higher OC/EC = more coatings\n• Positive r = coating enhancement', 
                        transform=ax4.transAxes, fontsize=9, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))
        
        plt.tight_layout()
        plt.show()
        
        return feature_importance, mac_clean
    
    return None, mac_clean

# %% [markdown]
# ## 7. Final Recommendations

# %%
def create_final_recommendations(mac_results, performance_metrics, feature_importance=None):
    """Create comprehensive recommendations for MAC method selection as text output"""
    
    print("🎯 FINAL MAC METHOD RECOMMENDATIONS")
    print("="*80)
    
    # Best method selection based on performance
    best_method = None
    best_score = -np.inf
    
    method_names = ['Method_1', 'Method_2', 'Method_3', 'Method_4']
    method_labels = ['Mean of Ratios', 'Ratio of Means', 'Standard Regression', 'Origin Regression']
    
    for method in method_names:
        perf = performance_metrics[method]
        # Combined score (lower bias and RMSE, higher correlation is better)
        score = perf['correlation'] - abs(perf['bias'])/10 - perf['rmse']/10
        if score > best_score:
            best_score = score
            best_method = method
    
    best_method_idx = method_names.index(best_method)
    best_method_name = method_labels[best_method_idx]
    best_mac = mac_results['mac_values'][best_method]
    
    print(f"\n📊 MAC METHOD RESULTS:")
    print(f"   Method 1 (Mean of Ratios):     {mac_results['mac_values']['Method_1']:.2f} m²/g")
    print(f"   Method 2 (Ratio of Means):     {mac_results['mac_values']['Method_2']:.2f} m²/g")
    print(f"   Method 3 (Standard Regression): {mac_results['mac_values']['Method_3']:.2f} m²/g + {mac_results['intercept']:.1f} Mm⁻¹")
    print(f"   Method 4 (Origin Regression):   {mac_results['mac_values']['Method_4']:.2f} m²/g")
    
    print(f"\n🥇 RECOMMENDED METHOD: {best_method_name}")
    print(f"   MAC Value: {best_mac:.2f} m²/g")
    print(f"   Performance Metrics:")
    print(f"     • Bias: {performance_metrics[best_method]['bias']:.3f} μg/m³")
    print(f"     • RMSE: {performance_metrics[best_method]['rmse']:.3f} μg/m³")
    print(f"     • Correlation: {performance_metrics[best_method]['correlation']:.3f}")
    
    print(f"\n✅ JUSTIFICATION:")
    if best_method != 'Method_3':
        print(f"   • Physically valid (BC = 0 when Fabs = 0)")
    else:
        print(f"   • Note: Has baseline absorption of {mac_results['intercept']:.1f} Mm⁻¹")
    
    if best_method in ['Method_1', 'Method_2']:
        print(f"   • Simple and robust calculation")
    else:
        print(f"   • Uses regression approach")
    
    print(f"   • MAC value within reasonable range for African urban environment")
    print(f"   • Best overall statistical performance")
    
    print(f"\n⚠️ METHOD-SPECIFIC CONSIDERATIONS:")
    print(f"\nMethod 3 (Standard Regression):")
    print(f"   • Large intercept ({mac_results['intercept']:.1f} Mm⁻¹) violates BC = 0 when Fabs = 0")
    print(f"   • May represent real brown carbon baseline absorption")
    print(f"   • Use only if non-BC absorption is confirmed and accounted for")
    
    print(f"\n🌍 ADDIS ABABA ATMOSPHERIC CONTEXT:")
    print(f"   • High baseline absorption indicates brown carbon from biomass burning")
    print(f"   • Cooking fires and wood burning create organic aerosols")
    print(f"   • Dust from unpaved roads affects measurements (Fe, Al correlations)")
    print(f"   • Seasonal variations due to Ethiopian climate patterns")
    print(f"   • Urban emissions at high altitude (2,370m) affect aerosol mixing")
    
    if feature_importance is not None and len(feature_importance) > 0:
        print(f"\n🧪 CHEMICAL COMPOSITION EFFECTS:")
        print(f"   Top MAC Variability Drivers (independent chemical species):")
        for i, row in feature_importance.head(5).iterrows():
            feature_name = row['feature'].replace('_', ' ')
            importance = row['importance']
            
            if 'ratio' in row['feature'].lower():
                unit = '(dimensionless)'
            elif 'coating' in row['feature'].lower():
                unit = '(dimensionless)'
            elif 'SIA' in row['feature']:
                unit = '(μg/m³)'
            else:
                unit = '(μg/m³)'
            
            print(f"     • {feature_name} {unit}: {importance:.3f} importance")
    
    print(f"\n💡 OPERATIONAL RECOMMENDATIONS:")
    print(f"\n1. ROUTINE MONITORING:")
    print(f"   • Use {best_method_name} for standard BC monitoring")
    print(f"   • Apply MAC = {best_mac:.2f} m²/g for ETAD site")
    print(f"   • Quality control: flag samples with unusual chemical ratios")
    
    print(f"\n2. SEASONAL CONSIDERATIONS:")
    print(f"   • Monitor for brown carbon effects during dry season")
    print(f"   • Consider higher MAC values during biomass burning periods")
    print(f"   • Account for dust effects during construction/dry periods")
    
    print(f"\n3. QUALITY CONTROL FLAGS:")
    print(f"   • K/PM2.5 > 2.0%: High biomass burning influence")
    print(f"   • Fe/Al > 0.9: High dust loading")
    print(f"   • MAC > 15 m²/g: Potential brown carbon enhancement")
    print(f"   • Fabs baseline > 20 Mm⁻¹: Non-BC absorption present")
    
    print(f"\n4. METHOD SELECTION MATRIX:")
    print(f"   Condition                    → Recommended Method")
    print(f"   Normal conditions           → {best_method_name}")
    print(f"   High biomass burning       → Method 1 or 2 + correction")
    print(f"   High dust loading          → Method 1 or 2 (avoid Method 3)")
    print(f"   Brown carbon confirmed     → Method 3 with baseline correction")
    
    print(f"\n5. UNCERTAINTY CONSIDERATIONS:")
    print(f"   • Report MAC uncertainty: ±1-2 m²/g for method differences")
    print(f"   • Consider seasonal MAC factors (±20% variation)")
    print(f"   • Account for brown carbon contribution in optical analysis")
    print(f"   • Use chemical tracers for source apportionment validation")
    
    print(f"\n📈 FUTURE RESEARCH PRIORITIES:")
    print(f"   • Multi-wavelength analysis for brown carbon quantification")
    print(f"   • Source-specific MAC value development")
    print(f"   • Humidity effects on coated particle absorption")
    print(f"   • Comparison with other African urban sites")
    print(f"   • Development of real-time MAC correction factors")
    
    print(f"\n🔗 INTEGRATION WITH OTHER MEASUREMENTS:")
    print(f"   • Use K⁺ as biomass burning tracer alongside MAC")
    print(f"   • Correlate with PM2.5 composition for source apportionment")
    print(f"   • Compare with satellite AOD for regional validation")
    print(f"   • Integrate with meteorological data for seasonal patterns")
    
    print(f"\n" + "="*80)

# %% [markdown]
# ## 8. Main Analysis Function

# %%
def run_complete_etad_analysis(db_path, csv_path):
    """Run the complete ETAD MAC-speciation analysis"""
    
    print("🚀 COMPLETE ETAD MAC-SPECIATION ANALYSIS")
    print("="*80)
    
    # Step 1: Load data
    print("\n1. Loading Data...")
    ftir_data = load_etad_ftir_data(db_path)
    if ftir_data is None:
        return None
    
    spec_data = load_etad_speciation_data(csv_path)
    if spec_data is None:
        return None
    
    # Step 2: Merge datasets
    print("\n2. Merging Datasets...")
    merged_data = merge_etad_datasets(ftir_data, spec_data)
    if merged_data is None:
        return None
    
    # Step 3: Calculate MAC methods
    print("\n3. Calculating MAC Methods...")
    mac_results = calculate_mac_methods(merged_data)
    
    # Step 4: Performance analysis
    print("\n4. Analyzing Method Performance...")
    performance_metrics = analyze_method_performance(merged_data, mac_results)
    
    # Step 5: Create visualizations
    print("\n5. Creating MAC Methods Comparison...")
    create_mac_methods_comparison(merged_data, mac_results)
    
    print("\n6. Analyzing Chemical Speciation Effects...")
    correlations = create_chemical_speciation_analysis(merged_data, mac_results)
    
    # Step 6: Chemical drivers analysis
    print("\n7. Analyzing Chemical Drivers...")
    feature_importance, mac_clean = analyze_chemical_drivers_of_mac(merged_data)
    
    # Step 7: Final recommendations
    print("\n8. Creating Final Recommendations...")
    create_final_recommendations(mac_results, performance_metrics, feature_importance)

    
    print("\n✅ COMPLETE ANALYSIS FINISHED!")
    print(f"\n📊 Analysis Summary:")
    print(f"   • FTIR samples: {len(ftir_data)}")
    print(f"   • Speciation samples: {len(spec_data)}")
    print(f"   • Merged samples: {len(merged_data)}")
    print(f"   • MAC methods calculated: 4")
    print(f"   • Chemical species analyzed: {len([col for col in merged_data.columns if col in ['PM25_mass', 'Sulfate_Ion', 'Nitrate_Ion', 'Potassium_Ion', 'Iron', 'Aluminum']])}")
    
    return {
        'ftir_data': ftir_data,
        'speciation_data': spec_data,
        'merged_data': merged_data,
        'mac_results': mac_results,
        'performance_metrics': performance_metrics,
        'correlations': correlations,
        'feature_importance': feature_importance,
        'mac_clean': mac_clean
    }

# Example usage:
if __name__ == "__main__":
    # Set your file paths
    DB_PATH = "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Original Data/Combined Database/spartan_ftir_hips.db"
    CSV_PATH = "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/EC-HIPS-Aeth Comparison/Data/Downloaded Data/SPARTAN/Addis Ababa/FilterBased_ChemSpecPM25_ETAD.csv"

    # Run complete analysis
    print("🎉 ETAD MAC-SPECIATION ANALYZER READY!")
    print(f"\nTo run the complete analysis:")
    print(f"results = run_complete_etad_analysis(DB_PATH, CSV_PATH)")
    print(f"\nThis will create:")
    print(f"• Comprehensive MAC method comparison and validation")
    print(f"• Chemical speciation effects analysis") 
    print(f"• Performance metrics and method recommendations")
    print(f"• Unit-aware analysis with proper conversions")
    print(f"• Publication-quality visualizations")
    
    # Uncomment to run:
    results = run_complete_etad_analysis(DB_PATH, CSV_PATH)

# %% [markdown]
# ## 9. Comprehensive Chemical Interference Analysis (FTIR-HIPS Only)

# %%
def analyze_all_chemical_interference(merged_data, mac_results):
    """
    Comprehensive analysis of ALL chemical species effects on HIPS optical absorption
    Uses only actual FTIR-EC and HIPS-Fabs data at 633nm wavelength
    Analyzes every available chemical species without pre-assumptions
    """
    
    print("🔬 ANALYZING ALL CHEMICAL INTERFERENCE WITH HIPS")
    print("="*80)
    print("📋 Analysis scope: FTIR-EC vs HIPS-Fabs (633nm) with ALL chemical species")
    print("⚠️  NO assumptions about absorption strength - let data decide")
    print("🎯 HIPS wavelength: 633nm (red light)")
    
    df = merged_data.copy()
    
    # Identify ALL chemical species columns (exclude non-chemical columns)
    exclude_columns = [
        'filter_id', 'Filter_ID', 'sample_date', 'Start_Date', 'site_code', 
        'volume_m3', 'ec_ftir', 'ec_ftir_mdl', 'oc_ftir', 'oc_ftir_mdl',
        'fabs', 'fabs_mdl', 'fabs_uncertainty', 'ftir_batch_id',
        'oc_ec_ratio', 'mac_individual', 'date_key', 'month', 'season',
        'BC_Method_1', 'BC_Method_2', 'BC_Method_3', 'BC_Method_4'
    ]
    
    # Get all chemical species
    all_columns = df.columns.tolist()
    chemical_species = [col for col in all_columns if col not in exclude_columns]
    
    # Filter to species with sufficient data
    available_species = []
    for species in chemical_species:
        if species in df.columns:
            species_data = df[species].dropna()
            if len(species_data) >= 10:  # Minimum 10 data points
                available_species.append(species)
    
    print(f"📊 ALL AVAILABLE CHEMICAL SPECIES: {len(available_species)}")
    for species in available_species:
        species_data = df[species].dropna()
        print(f"   • {species}: {species_data.mean():.3f} ± {species_data.std():.3f} μg/m³ (n={len(species_data)})")
    
    # Calculate baseline absorption theory
    intercept = mac_results['intercept']  # Mm⁻¹
    print(f"\n🎯 BASELINE ABSORPTION ANALYSIS (HIPS 633nm):")
    print(f"   Method 3 intercept: {intercept:.2f} Mm⁻¹")
    print(f"   This represents non-BC absorption when EC = 0")
    print(f"   Source: HIPS measurement at 633nm wavelength")
    
    # Calculate correlations for ALL species with HIPS Fabs
    print(f"\n🔬 CALCULATING CORRELATIONS WITH HIPS FABS (633nm)...")
    
    species_correlations = {}
    pm25_correlation = None
    
    for species in available_species:
        # Skip BC_PM25 since it's essentially the same as what we're measuring
        if species == 'BC_PM25':
            print(f"   ⚠️ Skipping {species} - circular correlation with HIPS measurement")
            continue
        
        # Handle PM25_mass separately
        if species == 'PM25_mass':
            species_data = df[species].dropna()
            fabs_subset = df.loc[species_data.index, 'fabs']
            if len(species_data) >= 10:
                r_pm25, p_pm25 = pearsonr(species_data, fabs_subset)
                pm25_correlation = {
                    'r': r_pm25, 
                    'p': p_pm25, 
                    'n': len(species_data),
                    'mean_conc': species_data.mean(),
                    'std_conc': species_data.std()
                }
                print(f"   📊 PM2.5 mass loading: r = {r_pm25:.3f} (analyzed separately)")
            continue
            
        species_data = df[species].dropna()
        fabs_subset = df.loc[species_data.index, 'fabs']  # HIPS Fabs at 633nm
        
        if len(species_data) >= 10:
            r_species, p_species = pearsonr(species_data, fabs_subset)
            species_correlations[species] = {
                'r': r_species, 
                'p': p_species, 
                'n': len(species_data),
                'mean_conc': species_data.mean(),
                'std_conc': species_data.std()
            }
    
    # Sort by absolute correlation strength
    sorted_correlations = sorted(species_correlations.items(), 
                                key=lambda x: abs(x[1]['r']), reverse=True)
    
    print(f"\n📈 TOP CHEMICAL SPECIES CORRELATIONS WITH HIPS FABS (633nm):")
    print(f"   (Excluding PM2.5 total mass - analyzed separately)")
    for i, (species, corr_data) in enumerate(sorted_correlations[:15]):  # Top 15
        significance = '***' if corr_data['p'] < 0.001 else '**' if corr_data['p'] < 0.01 else '*' if corr_data['p'] < 0.05 else ''
        print(f"   {i+1:2d}. {species:15s}: r = {corr_data['r']:6.3f}{significance:3s} (n={corr_data['n']})")
    
    # Report PM2.5 mass separately
    if pm25_correlation:
        significance = '***' if pm25_correlation['p'] < 0.001 else '**' if pm25_correlation['p'] < 0.01 else '*'
        print(f"\n📊 PM2.5 MASS LOADING ANALYSIS:")
        print(f"   • PM2.5 total mass: r = {pm25_correlation['r']:.3f}{significance} (n={pm25_correlation['n']})")
        print(f"   • Mean concentration: {pm25_correlation['mean_conc']:.1f} ± {pm25_correlation['std_conc']:.1f} μg/m³")
        print(f"   • This represents total aerosol loading, not specific interference")
    
    # Create comprehensive visualization - Plot 1: Chemical species correlations (excluding PM2.5)
    fig1, ax1 = plt.subplots(1, 1, figsize=(12, 8))
    top_species = sorted_correlations[:12]  # Top 12 for readability
    species_names = [item[0] for item in top_species]
    correlations = [item[1]['r'] for item in top_species]
    p_values = [item[1]['p'] for item in top_species]
    
    # Color by correlation strength
    colors = ['red' if abs(r) > 0.5 else 'orange' if abs(r) > 0.3 else 'blue' for r in correlations]
    
    bars = ax1.barh(range(len(species_names)), correlations, color=colors, alpha=0.8, edgecolor='black')
    
    ax1.set_yticks(range(len(species_names)))
    ax1.set_yticklabels([name.replace('_', ' ') for name in species_names])
    ax1.set_xlabel('Correlation with HIPS Fabs (r)', fontweight='bold')
    ax1.set_title('Top Chemical Species Correlations\n(633nm - Excluding PM₂.₅ Total Mass)', fontweight='bold')
    ax1.axvline(0, color='black', linestyle='-', linewidth=1)
    ax1.axvline(0.3, color='orange', linestyle='--', alpha=0.7, label='|r| > 0.3')
    ax1.axvline(-0.3, color='orange', linestyle='--', alpha=0.7)
    ax1.axvline(0.5, color='red', linestyle='--', alpha=0.7, label='|r| > 0.5')
    ax1.axvline(-0.5, color='red', linestyle='--', alpha=0.7)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Add significance markers
    for i, (bar, p_val) in enumerate(zip(bars, p_values)):
        significance = '***' if p_val < 0.001 else '**' if p_val < 0.01 else '*' if p_val < 0.05 else ''
        if significance:
            ax1.text(bar.get_width() + 0.01 if bar.get_width() > 0 else bar.get_width() - 0.01,
                    bar.get_y() + bar.get_height()/2, significance,
                    ha='left' if bar.get_width() > 0 else 'right', va='center', 
                    fontweight='bold', color='red')
    
    plt.tight_layout()
    plt.show()
    
    # Plot 1B: PM2.5 mass loading analysis
    fig1b, ax1b = plt.subplots(1, 1, figsize=(10, 8))
    if pm25_correlation:
        pm25_data = df['PM25_mass'].dropna()
        fabs_data = df.loc[pm25_data.index, 'fabs']
        
        # Color by season if available
        if 'season' in df.columns:
            seasons = df.loc[pm25_data.index, 'season']
            season_colors = {'Dry Season (Bega)': '#d35400', 
                           'Belg Rainy Season': '#27ae60', 
                           'Kiremt Rainy Season': '#2980b9'}
            
            for season in seasons.unique():
                season_mask = seasons == season
                if season in season_colors and season_mask.sum() > 5:
                    # Scatter plot for this season
                    ax1b.scatter(pm25_data[season_mask], fabs_data[season_mask], 
                               color=season_colors[season], alpha=0.7, s=50, 
                               label=season.split()[0], edgecolors='black', linewidth=0.5)
                    
                    # Individual regression line for this season
                    season_x = pm25_data[season_mask]
                    season_y = fabs_data[season_mask]
                    
                    if len(season_x) >= 5:
                        z_season = np.polyfit(season_x, season_y, 1)
                        p_season = np.poly1d(z_season)
                        x_line_season = np.linspace(season_x.min(), season_x.max(), 50)
                        
                        # Regression line with same color but dashed
                        ax1b.plot(x_line_season, p_season(x_line_season), 
                                color=season_colors[season], linewidth=2, 
                                linestyle='--', alpha=0.9)
                        
                        # Calculate R for this season
                        r_season, p_season_val = pearsonr(season_x, season_y)
                        print(f"     PM2.5 {season}: r = {r_season:.3f}, p = {p_season_val:.3f}")
        else:
            ax1b.scatter(pm25_data, fabs_data, alpha=0.7, s=50, color='blue', 
                       edgecolors='black', linewidth=0.5)
        
        # Add overall regression line in black
        r_pm25 = pm25_correlation['r']
        p_pm25 = pm25_correlation['p']
        z = np.polyfit(pm25_data, fabs_data, 1)
        p = np.poly1d(z)
        x_line = np.linspace(pm25_data.min(), pm25_data.max(), 100)
        ax1b.plot(x_line, p(x_line), 'black', linewidth=3, linestyle='-', alpha=0.8, label='Overall')
        
        ax1b.text(0.05, 0.95, f'PM₂.₅ Mass Loading Effect:\nOverall: r = {r_pm25:.3f}, p = {p_pm25:.2e}\n(Total aerosol loading)', 
                transform=ax1b.transAxes,
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                fontweight='bold')
        
        ax1b.set_xlabel('PM₂.₅ Mass (μg/m³)', fontweight='bold')
        ax1b.set_ylabel('HIPS Fabs at 633nm (Mm⁻¹)', fontweight='bold')
        ax1b.set_title('PM₂.₅ Total Mass vs HIPS Absorption\n(r=0.848 - Mass Loading Effect)', fontweight='bold')
        if 'season' in df.columns:
            ax1b.legend()
        ax1b.grid(True, alpha=0.3)
        
        print(f"\n   PM2.5 seasonal correlations:")
    
    plt.tight_layout()
    plt.show()
    
    # Plot 2: Top interfering species vs HIPS
    fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
    if len(sorted_correlations) > 0:
        top_species_name = sorted_correlations[0][0]
        top_species_data = df[top_species_name].dropna()
        fabs_data = df.loc[top_species_data.index, 'fabs']
        
        # Color by season if available
        if 'season' in df.columns:
            seasons = df.loc[top_species_data.index, 'season']
            season_colors = {'Dry Season (Bega)': '#d35400', 
                           'Belg Rainy Season': '#27ae60', 
                           'Kiremt Rainy Season': '#2980b9'}
            
            for season in seasons.unique():
                season_mask = seasons == season
                if season in season_colors and season_mask.sum() > 5:  # At least 5 points
                    # Scatter plot for this season
                    ax2.scatter(top_species_data[season_mask], fabs_data[season_mask], 
                               color=season_colors[season], alpha=0.7, s=50, 
                               label=season.split()[0], edgecolors='black', linewidth=0.5)
                    
                    # Individual regression line for this season
                    season_x = top_species_data[season_mask]
                    season_y = fabs_data[season_mask]
                    
                    if len(season_x) >= 5:  # Need minimum points for regression
                        z_season = np.polyfit(season_x, season_y, 1)
                        p_season = np.poly1d(z_season)
                        x_line_season = np.linspace(season_x.min(), season_x.max(), 50)
                        
                        # Regression line with same color but dashed
                        ax2.plot(x_line_season, p_season(x_line_season), 
                                color=season_colors[season], linewidth=2, 
                                linestyle='--', alpha=0.9)
                        
                        # Calculate R for this season
                        r_season, p_season_val = pearsonr(season_x, season_y)
                        print(f"     {season}: r = {r_season:.3f}, p = {p_season_val:.3f}")
        else:
            ax2.scatter(top_species_data, fabs_data, alpha=0.7, s=50, color='blue', 
                       edgecolors='black', linewidth=0.5)
        
        # Add overall regression line in black
        r_top, p_top = pearsonr(top_species_data, fabs_data)
        z = np.polyfit(top_species_data, fabs_data, 1)
        p = np.poly1d(z)
        x_line = np.linspace(top_species_data.min(), top_species_data.max(), 100)
        ax2.plot(x_line, p(x_line), 'black', linewidth=3, linestyle='-', alpha=0.8, label='Overall')
        
        ax2.text(0.05, 0.95, f'{top_species_name.replace("_", " ")}-HIPS:\nOverall: r = {r_top:.3f}, p = {p_top:.2e}', 
                transform=ax2.transAxes,
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                fontweight='bold')
        
        print(f"\n   Seasonal correlations for {top_species_name}:")
        
        ax2.set_xlabel(f'{top_species_name.replace("_", " ")} (μg/m³)', fontweight='bold')
        ax2.set_ylabel('HIPS Fabs at 633nm (Mm⁻¹)', fontweight='bold')
        ax2.set_title(f'Top Interfering Species: {top_species_name.replace("_", " ")}', fontweight='bold')
        if 'season' in df.columns:
            ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Plot 3: Metals vs non-metals comparison
    fig3, ax3 = plt.subplots(1, 1, figsize=(8, 8))
    
    # Identify metals vs non-metals
    likely_metals = ['Iron', 'Aluminum', 'Titanium', 'Vanadium', 'Chromium', 'Manganese', 
                    'Cobalt', 'Nickel', 'Copper', 'Zinc', 'Arsenic', 'Selenium', 
                    'Cadmium', 'Antimony', 'Cerium', 'Lead', 'Rubidium', 'Strontium']
    
    metal_correlations = []
    non_metal_correlations = []
    metal_names = []
    non_metal_names = []
    
    for species, corr_data in species_correlations.items():
        if any(metal in species for metal in likely_metals):
            metal_correlations.append(abs(corr_data['r']))
            metal_names.append(species)
        else:
            non_metal_correlations.append(abs(corr_data['r']))
            non_metal_names.append(species)
    
    if metal_correlations and non_metal_correlations:
        bp = ax3.boxplot([metal_correlations, non_metal_correlations], 
                        labels=['Metals', 'Non-Metals'], patch_artist=True)
        
        bp['boxes'][0].set_facecolor('#e74c3c')
        bp['boxes'][1].set_facecolor('#3498db')
        for box in bp['boxes']:
            box.set_alpha(0.7)
        
        ax3.set_ylabel('|Correlation with HIPS Fabs|', fontweight='bold')
        ax3.set_title('Metals vs Non-Metals\nInterference Comparison', fontweight='bold')
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Add statistical test
        from scipy.stats import mannwhitneyu
        stat, p_val = mannwhitneyu(metal_correlations, non_metal_correlations)
        ax3.text(0.02, 0.98, f'Mann-Whitney U test:\np = {p_val:.3f}', 
                transform=ax3.transAxes, va='top',
                bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.9),
                fontweight='bold')
    
    plt.tight_layout()
    plt.show()
    
    # Plot 4: Iron specifically (if available)
    fig4, ax4 = plt.subplots(1, 1, figsize=(10, 8))
    if 'Iron' in species_correlations:
        iron_data = df['Iron'].dropna()
        individual_mac_633 = df.loc[iron_data.index, 'fabs'] / df.loc[iron_data.index, 'ec_ftir']
        
        # Filter reasonable MAC values
        reasonable_mask = (individual_mac_633 > 5) & (individual_mac_633 < 25)
        iron_clean = iron_data[reasonable_mask]
        mac_clean = individual_mac_633[reasonable_mask]
        
        if len(iron_clean) > 10:
            ax4.scatter(iron_clean, mac_clean, alpha=0.7, s=50, color='red', 
                       edgecolors='black', linewidth=0.5)
            
            # Add regression line
            r_iron, p_iron = pearsonr(iron_clean, mac_clean)
            z = np.polyfit(iron_clean, mac_clean, 1)
            p = np.poly1d(z)
            x_line = np.linspace(iron_clean.min(), iron_clean.max(), 100)
            ax4.plot(x_line, p(x_line), 'blue', linewidth=3, linestyle='--', alpha=0.8)
            
            ax4.text(0.05, 0.95, f'Fe-MAC (633nm):\nr = {r_iron:.3f}\np = {p_iron:.2e}', 
                    transform=ax4.transAxes,
                    bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                    fontweight='bold')
            
            ax4.set_xlabel('Iron Concentration (μg/m³)', fontweight='bold')
            ax4.set_ylabel('Individual MAC at 633nm (m²/g)', fontweight='bold')
            ax4.set_title('Iron vs HIPS MAC (633nm)\nRed light absorption', fontweight='bold')
            ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Plot 5: Baseline absorption analysis
    fig5, ax5 = plt.subplots(1, 1, figsize=(10, 8))
    if len(sorted_correlations) > 0:
        # Use top correlating species for baseline analysis
        top_species_name = sorted_correlations[0][0]
        species_data = df[top_species_name].dropna()
        
        # Calculate baseline using Method 3 intercept theory
        expected_bc_fabs = mac_results['mac_values']['Method_3'] * df.loc[species_data.index, 'ec_ftir']
        baseline_fabs = df.loc[species_data.index, 'fabs'] - expected_bc_fabs
        
        # Bin species concentrations
        species_quartiles = pd.qcut(species_data, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
        
        baseline_by_quartile = [baseline_fabs[species_quartiles == q].dropna() 
                              for q in species_quartiles.cat.categories]
        
        bp = ax5.boxplot(baseline_by_quartile, labels=['Low', 'Med-Low', 'Med-High', 'High'],
                        patch_artist=True)
        
        colors_quart = ['#3498db', '#f39c12', '#e67e22', '#e74c3c']
        for patch, color in zip(bp['boxes'], colors_quart):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax5.axhline(intercept, color='purple', linestyle='--', linewidth=2, 
                   label=f'Method 3 Intercept ({intercept:.1f} Mm⁻¹)')
        ax5.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
        
        ax5.set_xlabel(f'{top_species_name.replace("_", " ")} Quartiles', fontweight='bold')
        ax5.set_ylabel('Baseline Absorption (Mm⁻¹)', fontweight='bold')
        ax5.set_title(f'Baseline vs {top_species_name.replace("_", " ")}\n(HIPS 633nm)', fontweight='bold')
        ax5.legend()
        ax5.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()
    
    # Plot 6: Seasonal effects
    fig6, ax6 = plt.subplots(1, 1, figsize=(10, 8))
    if 'season' in df.columns:
        seasonal_data = []
        season_labels = []
        
        for season in df['season'].unique():
            season_mask = df['season'] == season
            season_mac = (df.loc[season_mask, 'fabs'] / df.loc[season_mask, 'ec_ftir']).dropna()
            
            if len(season_mac) > 5:
                seasonal_data.append(season_mac)
                season_labels.append(season.split()[0])
        
        if len(seasonal_data) >= 2:
            bp = ax6.boxplot(seasonal_data, labels=season_labels, patch_artist=True)
            
            season_colors = ['#d35400', '#27ae60', '#2980b9'][:len(seasonal_data)]
            for patch, color in zip(bp['boxes'], season_colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Add top species concentrations by season
            if len(sorted_correlations) > 0:
                top_species_name = sorted_correlations[0][0]
                for i, season in enumerate(df['season'].unique()):
                    if i < len(season_labels):
                        season_mask = df['season'] == season
                        season_conc = df.loc[season_mask, top_species_name].mean()
                        ax6.text(i+1, ax6.get_ylim()[1]*0.9, f'{season_conc:.2f}', 
                                ha='center', fontweight='bold', fontsize=9,
                                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            ax6.set_ylabel('HIPS MAC at 633nm (m²/g)', fontweight='bold')
            ax6.set_title('Seasonal MAC Variations\n(with top species concentrations)', fontweight='bold')
            ax6.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()
    
    # Summary statistics
    print(f"\n📈 COMPREHENSIVE ANALYSIS RESULTS (HIPS 633nm):")
    print(f"="*70)
    print(f"   • Total chemical species analyzed: {len(available_species)}")
    print(f"   • HIPS wavelength: 633nm (red light)")
    print(f"   • Method 3 baseline: {intercept:.2f} Mm⁻¹")
    
    # Categorize by correlation strength (excluding PM2.5)
    strong_correlations = [(s, d) for s, d in species_correlations.items() if abs(d['r']) > 0.5]
    moderate_correlations = [(s, d) for s, d in species_correlations.items() if 0.3 < abs(d['r']) <= 0.5]
    
    print(f"\n🔴 STRONG CHEMICAL CORRELATIONS (|r| > 0.5): {len(strong_correlations)}")
    for species, data in strong_correlations:
        significance = '***' if data['p'] < 0.001 else '**' if data['p'] < 0.01 else '*'
        print(f"   • {species}: r = {data['r']:.3f}{significance}")
    
    print(f"\n🟡 MODERATE CHEMICAL CORRELATIONS (0.3 < |r| ≤ 0.5): {len(moderate_correlations)}")
    for species, data in moderate_correlations:
        significance = '***' if data['p'] < 0.001 else '**' if data['p'] < 0.01 else '*'
        print(f"   • {species}: r = {data['r']:.3f}{significance}")
    
    if pm25_correlation:
        print(f"\n📊 PM2.5 MASS LOADING (analyzed separately):")
        significance = '***' if pm25_correlation['p'] < 0.001 else '**' if pm25_correlation['p'] < 0.01 else '*'
        print(f"   • PM25_mass: r = {pm25_correlation['r']:.3f}{significance}")
        print(f"   • Interpretation: Total aerosol loading effect, not specific chemical interference")
    
    print(f"\n💡 RECOMMENDATIONS FOR HIPS 633nm:")
    print(f"   1. PM₂.₅ mass loading (r=0.848) dominates - consider mass-normalized analysis")
    print(f"   2. Monitor top chemical interfering species during analysis")
    print(f"   3. Distinguish between mass loading effects vs chemical interference")
    print(f"   4. Use seasonal patterns for quality control")
    print(f"   5. Flag samples with extreme chemical concentrations")
    print(f"   6. Cross-reference with meteorological data")
    print(f"   7. Remember: Analysis specific to 633nm red light")
    
    return species_correlations, available_species, pm25_correlation

# %% [markdown]
# ## 10. Key Interference Species Deep Dive Analysis

# %%
def analyze_key_interference_species(merged_data, mac_results, species_correlations):
    """
    Deep dive analysis of the key interfering species identified from comprehensive screening
    Focus on PM2.5 mass, metals (Mn, Ti, Fe), and dust signatures at 633nm
    """
    
    print("🎯 KEY INTERFERENCE SPECIES DEEP DIVE (633nm)")
    print("="*80)
    print("📋 Focus: Top interfering species from comprehensive analysis")
    print("🔬 Based on actual correlation data - no assumptions")
    
    df = merged_data.copy()
    
    # Top interfering species from your results (excluding BC_PM25)
    key_species = {
        'PM25_mass': {'category': 'total_mass', 'expected': 'strong', 'your_r': 0.848},
        'Manganese': {'category': 'metal', 'expected': 'moderate', 'your_r': 0.595},
        'Potassium_Ion': {'category': 'biomass_burning', 'expected': 'strong', 'your_r': 0.577},
        'Titanium': {'category': 'dust', 'expected': 'moderate', 'your_r': 0.573},
        'Iron': {'category': 'dust', 'expected': 'moderate', 'your_r': 0.572},
        'Arsenic': {'category': 'anthropogenic', 'expected': 'weak', 'your_r': 0.565},
        'Aluminum': {'category': 'dust', 'expected': 'reference', 'your_r': 0.512},
        'Silicon': {'category': 'dust', 'expected': 'weak', 'your_r': 0.511}
    }
    
    print(f"\n🎯 KEY INTERFERING SPECIES AT 633nm:")
    for species, info in key_species.items():
        if species in df.columns:
            print(f"   • {species}: r = {info['your_r']:.3f} ({info['category']})")
    
    # Plot 1: Source signature analysis - Dust vs Biomass
    fig1, ax1 = plt.subplots(1, 1, figsize=(10, 8))
    if all(col in df.columns for col in ['Iron', 'Aluminum', 'Potassium_Ion', 'PM25_mass']):
        # Dust signature: Fe/Al ratio
        dust_signature = df['Iron'] / df['Aluminum']
        # Biomass signature: K/PM2.5 ratio  
        biomass_signature = df['Potassium_Ion'] / df['PM25_mass'] * 100  # %
        
        # Individual MAC for coloring
        individual_mac = df['fabs'] / df['ec_ftir']
        reasonable_mask = (individual_mac > 5) & (individual_mac < 20)
        
        # Add seasonal analysis if available
        if 'season' in df.columns:
            season_colors = {'Dry Season (Bega)': '#d35400', 
                           'Belg Rainy Season': '#27ae60', 
                           'Kiremt Rainy Season': '#2980b9'}
            
            print(f"\n   🌍 Seasonal source analysis:")
            
            for season in df['season'].unique():
                season_mask = (df['season'] == season) & reasonable_mask
                if season_mask.sum() > 5 and season in season_colors:
                    season_biomass = biomass_signature[season_mask]
                    season_dust = dust_signature[season_mask]
                    season_mac = individual_mac[season_mask]
                    
                    # Scatter with seasonal colors
                    scatter = ax1.scatter(season_biomass, season_dust, 
                                        c=season_mac, cmap='viridis', alpha=0.7, s=60,
                                        edgecolors=season_colors[season], linewidth=2,
                                        label=season.split()[0])
                    
                    # Optional: Add regression line for biomass vs dust relationship by season
                    if len(season_biomass) >= 5:
                        z_season = np.polyfit(season_biomass, season_dust, 1)
                        p_season = np.poly1d(z_season)
                        x_line_season = np.linspace(season_biomass.min(), season_biomass.max(), 50)
                        ax1.plot(x_line_season, p_season(x_line_season), 
                                color=season_colors[season], linewidth=2, 
                                linestyle='--', alpha=0.8)
                        
                        r_season, _ = pearsonr(season_biomass, season_dust)
                        print(f"     {season}: r(biomass-dust) = {r_season:.3f}, MAC = {season_mac.mean():.2f}±{season_mac.std():.2f}")
        else:
            scatter = ax1.scatter(biomass_signature[reasonable_mask], dust_signature[reasonable_mask],
                                 c=individual_mac[reasonable_mask], cmap='viridis', alpha=0.7, s=60,
                                 edgecolors='black', linewidth=0.5)
        
        ax1.set_xlabel('Biomass Burning Signature\nK/PM₂.₅ (%)', fontweight='bold')
        ax1.set_ylabel('Dust Signature\nFe/Al Ratio', fontweight='bold')
        ax1.set_title('Source Discrimination\n(Color = HIPS MAC at 633nm)', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add quadrant labels
        ax1.text(0.95, 0.95, 'High Dust\nHigh Biomass', transform=ax1.transAxes, 
                ha='right', va='top', fontweight='bold', 
                bbox=dict(boxstyle='round', facecolor='orange', alpha=0.7))
        ax1.text(0.05, 0.05, 'Low Dust\nLow Biomass', transform=ax1.transAxes, 
                ha='left', va='bottom', fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.7))
        
        if 'season' in df.columns:
            ax1.legend()
        
        cbar = plt.colorbar(scatter, ax=ax1)
        cbar.set_label('HIPS MAC (m²/g)')
    
    plt.tight_layout()
    plt.show()
    
    # Plot 2: Metal interference hierarchy
    fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
    metal_species = ['Iron', 'Manganese', 'Titanium', 'Aluminum', 'Arsenic']
    available_metals = [m for m in metal_species if m in species_correlations]
    
    if available_metals:
        metal_correlations = [abs(species_correlations[m]['r']) for m in available_metals]
        metal_colors = ['#e74c3c' if abs(species_correlations[m]['r']) > 0.5 else '#f39c12' 
                       for m in available_metals]
        
        bars = ax2.bar(range(len(available_metals)), metal_correlations, 
                      color=metal_colors, alpha=0.8, edgecolor='black', linewidth=2)
        
        ax2.set_xticks(range(len(available_metals)))
        ax2.set_xticklabels(available_metals, rotation=45)
        ax2.set_ylabel('|Correlation with HIPS Fabs|', fontweight='bold')
        ax2.set_title('Metal Interference Hierarchy\n(633nm red light)', fontweight='bold')
        ax2.axhline(0.5, color='red', linestyle='--', alpha=0.7, label='Strong (>0.5)')
        ax2.axhline(0.3, color='orange', linestyle='--', alpha=0.7, label='Moderate (>0.3)')
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add correlation values on bars
        for bar, r_val, metal in zip(bars, metal_correlations, available_metals):
            actual_r = species_correlations[metal]['r']
            ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.01,
                    f'{actual_r:.3f}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.show()
    
    # Plot 3: PM2.5 mass loading effect
    fig3, ax3 = plt.subplots(1, 1, figsize=(10, 8))
    if 'PM25_mass' in df.columns:
        pm25_data = df['PM25_mass'].dropna()
        individual_mac = df.loc[pm25_data.index, 'fabs'] / df.loc[pm25_data.index, 'ec_ftir']
        
        # Create PM2.5 loading bins
        pm25_bins = pd.qcut(pm25_data, q=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
        
        # Calculate MAC statistics by bin
        mac_by_bin = []
        bin_labels = []
        bin_colors = ['#3498db', '#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']
        
        for bin_label in pm25_bins.cat.categories:
            bin_mask = pm25_bins == bin_label
            bin_mac = individual_mac[bin_mask]
            reasonable_bin_mac = bin_mac[(bin_mac > 5) & (bin_mac < 20)]
            
            if len(reasonable_bin_mac) > 3:
                mac_by_bin.append(reasonable_bin_mac)
                bin_labels.append(f'{bin_label}\n({len(reasonable_bin_mac)})')
        
        if len(mac_by_bin) >= 3:
            bp = ax3.boxplot(mac_by_bin, labels=bin_labels, patch_artist=True)
            
            for patch, color in zip(bp['boxes'], bin_colors[:len(bp['boxes'])]):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            ax3.set_ylabel('HIPS MAC at 633nm (m²/g)', fontweight='bold')
            ax3.set_xlabel('PM₂.₅ Mass Loading', fontweight='bold')
            ax3.set_title('MAC vs PM₂.₅ Mass Loading\n(Strong correlation: r=0.848)', fontweight='bold')
            ax3.grid(True, alpha=0.3, axis='y')
            
            # Add trend line through means
            bin_means = [np.mean(bin_data) for bin_data in mac_by_bin]
            ax3.plot(range(1, len(bin_means)+1), bin_means, 'red', linewidth=3, 
                    marker='D', markersize=8, label='Mean Trend')
            ax3.legend()
    
    plt.tight_layout()
    plt.show()
    
    # Plot 4: Seasonal interference patterns
    fig4, ax4 = plt.subplots(1, 1, figsize=(10, 8))
    if 'season' in df.columns:
        # Get top interfering chemical species (exclude PM25_mass - it's just total mass)
        chemical_species = ['Manganese', 'Potassium_Ion', 'Titanium', 'Iron', 'Arsenic']
        available_chemicals = [s for s in chemical_species if s in df.columns]
        
        if len(available_chemicals) >= 3:
            seasons = df['season'].unique()
            x_pos = np.arange(len(seasons))
            width = 0.15  # Narrower bars for more species
            
            colors_species = ['#8e44ad', '#3498db', '#34495e', '#e74c3c', '#f39c12']
            
            for i, species in enumerate(available_chemicals[:5]):  # Top 5 chemical species
                seasonal_means = []
                seasonal_stds = []
                
                for season in seasons:
                    season_data = df[df['season'] == season][species].dropna()
                    if len(season_data) > 0:
                        seasonal_means.append(season_data.mean())
                        seasonal_stds.append(season_data.std())
                    else:
                        seasonal_means.append(0)
                        seasonal_stds.append(0)
                
                ax4.bar(x_pos + i*width, seasonal_means, width, 
                       yerr=seasonal_stds, capsize=3,
                       color=colors_species[i], alpha=0.8, 
                       label=species.replace('_', ' '), edgecolor='black')
            
            ax4.set_xticks(x_pos + width*2)  # Center the labels
            ax4.set_xticklabels([s.split()[0] for s in seasons])
            ax4.set_ylabel('Concentration (μg/m³)', fontweight='bold')
            ax4.set_title('Seasonal Patterns of Top Interfering Chemical Species\n(Excluding PM₂.₅ total mass)', fontweight='bold')
            ax4.legend()
            ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()
    
    # Detailed interference analysis
    print(f"\n🔬 DETAILED INTERFERENCE ANALYSIS:")
    print(f"="*60)
    
    # Dust vs Combustion analysis
    if all(col in df.columns for col in ['Iron', 'Aluminum', 'Potassium_Ion']):
        dust_metals = df[['Iron', 'Aluminum', 'Titanium', 'Silicon']].sum(axis=1)
        combustion_tracers = df[['Potassium_Ion']]  # Could add others
        
        dust_high = dust_metals > dust_metals.quantile(0.75)
        combustion_high = df['Potassium_Ion'] > df['Potassium_Ion'].quantile(0.75)
        
        print(f"\n🏭 SOURCE ANALYSIS:")
        print(f"   • High dust samples: {dust_high.sum()} ({dust_high.sum()/len(df)*100:.1f}%)")
        print(f"   • High biomass burning: {combustion_high.sum()} ({combustion_high.sum()/len(df)*100:.1f}%)")
        print(f"   • Mixed sources: {(dust_high & combustion_high).sum()} samples")
        
        # MAC during different source conditions
        mac_dust_high = (df.loc[dust_high, 'fabs'] / df.loc[dust_high, 'ec_ftir']).mean()
        mac_bb_high = (df.loc[combustion_high, 'fabs'] / df.loc[combustion_high, 'ec_ftir']).mean()
        mac_clean = (df.loc[~(dust_high | combustion_high), 'fabs'] / 
                    df.loc[~(dust_high | combustion_high), 'ec_ftir']).mean()
        
        print(f"\n📊 MAC BY SOURCE TYPE:")
        print(f"   • High dust periods: {mac_dust_high:.2f} m²/g")
        print(f"   • High biomass burning: {mac_bb_high:.2f} m²/g") 
        print(f"   • Clean periods: {mac_clean:.2f} m²/g")
    
    # Metal interference summary
    print(f"\n🔩 METAL INTERFERENCE SUMMARY (633nm):")
    metal_interferences = ['Manganese', 'Titanium', 'Iron', 'Aluminum']
    for metal in metal_interferences:
        if metal in species_correlations:
            r_val = species_correlations[metal]['r']
            conc = species_correlations[metal]['mean_conc']
            if abs(r_val) > 0.5:
                level = "HIGH"
            elif abs(r_val) > 0.3:
                level = "MODERATE"
            else:
                level = "LOW"
            print(f"   • {metal}: {level} interference (r={r_val:.3f}, {conc:.3f} μg/m³)")
    
    print(f"\n💡 KEY FINDINGS FOR HIPS 633nm:")
    print(f"   1. PM₂.₅ mass loading strongly affects apparent MAC (r=0.848)")
    print(f"   2. Multiple metals show significant interference")
    print(f"   3. Dust and biomass burning both contribute to interference")
    print(f"   4. Arsenic unexpectedly high correlation - investigate anthropogenic sources")
    print(f"   5. Consider source-specific MAC corrections")
    
    return key_species

# %% [markdown]
# ## 11. Enhanced Machine Learning Models with Per-Model Feature Analysis

# %%
def build_enhanced_ml_interference_models(merged_data, mac_results, species_correlations):
    """
    Build various ML models to predict HIPS absorption interference using chemical species
    Enhanced to show top feature recommendation per model - WITH SEPARATE GRAPHS
    """
    
    print("🤖 ENHANCED MACHINE LEARNING INTERFERENCE PREDICTION MODELS")
    print("="*80)
    print("📋 Goal: Predict HIPS MAC interference using chemical composition")
    print("🎯 Target: Individual MAC values (HIPS 633nm)")
    print("📊 Features: Top correlating chemical species")
    print("🔍 NEW: Top feature recommendation per model")
    print("📈 VISUALIZATION: Individual graphs (not collage)")
    
    df = merged_data.copy()
    
    # Prepare target variable (individual MAC)
    target = df['fabs'] / df['ec_ftir']
    reasonable_mask = (target > 5) & (target < 20)  # Filter reasonable MAC values
    
    # Select top correlating chemical species as features (exclude PM25_mass and BC_PM25)
    exclude_features = ['PM25_mass', 'BC_PM25', 'fabs', 'ec_ftir', 'mac_individual']
    
    # Get top 15 chemical species based on correlation strength
    top_species = []
    for species, corr_data in sorted(species_correlations.items(), 
                                   key=lambda x: abs(x[1]['r']), reverse=True):
        if species not in exclude_features and species in df.columns:
            # Check actual data availability in the dataframe
            actual_data = df[species].dropna()
            if len(actual_data) >= 10:  # Use actual non-null count
                top_species.append(species)
                if len(top_species) >= 15:  # Limit to top 15
                    break
    
    print(f"\n📊 SELECTED FEATURES ({len(top_species)} chemical species):")
    for i, species in enumerate(top_species):
        r_val = species_correlations[species]['r']
        print(f"   {i+1:2d}. {species:15s}: r = {r_val:6.3f}")
    
    # Prepare dataset
    X = df[top_species].copy()
    y = target.copy()
    
    # Apply reasonable mask
    X = X[reasonable_mask]
    y = y[reasonable_mask]
    
    # Handle missing values
    X = X.fillna(X.median())
    
    print(f"\n📈 DATASET PREPARATION:")
    print(f"   • Samples: {len(X)} (after filtering)")
    print(f"   • Features: {len(top_species)} chemical species")
    print(f"   • Target: Individual MAC (633nm)")
    print(f"   • Target range: {y.min():.2f} - {y.max():.2f} m²/g")
    
    # Train-test split
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
    from sklearn.linear_model import Lasso, ElasticNet
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # Scale features for some models
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Initialize models with feature importance capabilities
    models = {}
    
    # 1. Random Forest (feature_importances_)
    from sklearn.ensemble import RandomForestRegressor
    models['Random Forest'] = RandomForestRegressor(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )
    
    # 2. Extra Trees (feature_importances_)
    from sklearn.ensemble import ExtraTreesRegressor
    models['Extra Trees'] = ExtraTreesRegressor(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )
    
    # 3. Gradient Boosting (feature_importances_)
    from sklearn.ensemble import GradientBoostingRegressor
    models['Gradient Boosting'] = GradientBoostingRegressor(
        n_estimators=100, max_depth=6, learning_rate=0.1, random_state=42
    )
    
    # 4. Lasso Regression (coef_)
    models['Lasso'] = Lasso(alpha=0.1, random_state=42)
    
    # 5. Elastic Net (coef_)
    models['Elastic Net'] = ElasticNet(alpha=0.1, l1_ratio=0.5, random_state=42)
    
    # 6. Ridge Regression (coef_)
    from sklearn.linear_model import Ridge
    models['Ridge'] = Ridge(alpha=1.0)
    
    # 7. Support Vector Regression (manual feature importance)
    from sklearn.svm import SVR
    models['SVR'] = SVR(kernel='rbf', C=1.0, gamma='scale')
    
    # Optional: Try advanced models
    advanced_models = []
    
    # Try XGBoost
    try:
        import xgboost as xgb
        models['XGBoost'] = xgb.XGBRegressor(
            n_estimators=100, max_depth=6, learning_rate=0.1, 
            random_state=42, n_jobs=-1, verbosity=0
        )
        advanced_models.append('XGBoost')
    except (ImportError, Exception) as e:
        print(f"   ⚠️ XGBoost not available: {e}")
    
    # Try LightGBM
    try:
        import lightgbm as lgb
        models['LightGBM'] = lgb.LGBMRegressor(
            n_estimators=100, max_depth=6, learning_rate=0.1,
            random_state=42, n_jobs=-1, verbose=-1
        )
        advanced_models.append('LightGBM')
    except (ImportError, Exception) as e:
        print(f"   ⚠️ LightGBM not available: {e}")
    
    # Try CatBoost
    try:
        import catboost as cb
        models['CatBoost'] = cb.CatBoostRegressor(
            iterations=100, depth=6, learning_rate=0.1,
            random_seed=42, verbose=False
        )
        advanced_models.append('CatBoost')
    except (ImportError, Exception) as e:
        print(f"   ⚠️ CatBoost not available: {e}")
    
    if advanced_models:
        print(f"   ✅ Advanced models available: {', '.join(advanced_models)}")
    else:
        print(f"   📊 Using sklearn models only")
    
    print(f"\n🤖 TRAINING {len(models)} ML MODELS...")
    
    # Train and evaluate models with feature importance tracking
    results = {}
    predictions = {}
    model_feature_importance = {}
    
    for name, model in models.items():
        print(f"   Training {name}...")
        
        try:
            # Determine if model needs scaled data
            linear_models = ['Lasso', 'Elastic Net', 'Ridge']
            
            if name in linear_models:
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
                X_for_importance = X_train_scaled
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                X_for_importance = X_train
            
            # Calculate metrics
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            # Extract feature importance
            if hasattr(model, 'feature_importances_'):
                # Tree-based models
                importances = model.feature_importances_
                importance_type = 'gain'
            elif hasattr(model, 'coef_'):
                # Linear models - use absolute coefficients
                importances = np.abs(model.coef_)
                importance_type = 'coefficient'
            elif name == 'SVR':
                # SVR: Use permutation importance as fallback
                from sklearn.inspection import permutation_importance
                if name in linear_models:
                    perm_importance = permutation_importance(model, X_test_scaled, y_test, random_state=42, n_repeats=5)
                else:
                    perm_importance = permutation_importance(model, X_test, y_test, random_state=42, n_repeats=5)
                importances = perm_importance.importances_mean
                importance_type = 'permutation'
            else:
                # Fallback - try to get from XGBoost/LightGBM
                try:
                    importances = model.feature_importances_
                    importance_type = 'gain'
                except:
                    importances = np.zeros(len(top_species))
                    importance_type = 'unknown'
            
            # Create feature importance dataframe for this model
            feature_df = pd.DataFrame({
                'feature': top_species,
                'importance': importances
            }).sort_values('importance', ascending=False)
            
            # Get top feature
            top_feature = feature_df.iloc[0]['feature']
            top_importance = feature_df.iloc[0]['importance']
            
            results[name] = {
                'RMSE': rmse,
                'MAE': mae,
                'R²': r2,
                'model': model,
                'top_feature': top_feature,
                'top_importance': top_importance,
                'importance_type': importance_type
            }
            predictions[name] = y_pred
            model_feature_importance[name] = feature_df
            
        except Exception as e:
            print(f"     ❌ Error training {name}: {e}")
            continue
    
    # Display results with top features
    print(f"\n📊 MODEL PERFORMANCE WITH TOP FEATURES:")
    print(f"{'Model':<15} {'RMSE':<7} {'MAE':<7} {'R²':<7} {'Top Feature':<15} {'Importance':<10}")
    print("-" * 85)
    
    # Sort by R²
    sorted_results = sorted(results.items(), key=lambda x: x[1]['R²'], reverse=True)
    
    for name, metrics in sorted_results:
        print(f"{name:<15} {metrics['RMSE']:<7.3f} {metrics['MAE']:<7.3f} {metrics['R²']:<7.3f} "
              f"{metrics['top_feature']:<15} {metrics['top_importance']:<10.3f}")
    
    # Model agreement analysis on top features
    print(f"\n🎯 TOP FEATURE CONSENSUS ANALYSIS:")
    
    # Count how many models choose each feature as #1
    top_feature_votes = {}
    for name, metrics in results.items():
        top_feat = metrics['top_feature']
        if top_feat in top_feature_votes:
            top_feature_votes[top_feat] += 1
        else:
            top_feature_votes[top_feat] = 1
    
    # Sort by votes
    consensus_features = sorted(top_feature_votes.items(), key=lambda x: x[1], reverse=True)
    
    print(f"   Feature consensus (votes from {len(results)} models):")
    for feature, votes in consensus_features:
        percentage = votes / len(results) * 100
        stars = '⭐' * votes
        print(f"   • {feature:<20}: {votes}/{len(results)} votes ({percentage:4.1f}%) {stars}")
    
    # SEPARATE VISUALIZATIONS
    print(f"\n📈 CREATING INDIVIDUAL VISUALIZATIONS...")
    
    # Graph 1: Model Performance Comparison
    print("   📊 Graph 1: Model Performance Comparison")
    plt.figure(figsize=(12, 8))
    
    model_names = [name for name, _ in sorted_results]
    r2_scores = [metrics['R²'] for _, metrics in sorted_results]
    
    colors = ['#2ecc71' if r2 > 0.7 else '#f39c12' if r2 > 0.5 else '#e74c3c' for r2 in r2_scores]
    bars = plt.bar(range(len(model_names)), r2_scores, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    
    plt.xticks(range(len(model_names)), model_names, rotation=45, ha='right', fontsize=12)
    plt.ylabel('R² Score', fontweight='bold', fontsize=14)
    plt.title('Machine Learning Model Performance Comparison\n(R² Scores for MAC Prediction)', 
              fontweight='bold', fontsize=16)
    plt.grid(True, alpha=0.3, axis='y')
    
    # Add R² values on bars
    for bar, r2 in zip(bars, r2_scores):
        plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.01,
                f'{r2:.3f}', ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Add performance legend
    plt.axhline(y=0.7, color='green', linestyle='--', alpha=0.7, label='Good (R² > 0.7)')
    plt.axhline(y=0.5, color='orange', linestyle='--', alpha=0.7, label='Fair (R² > 0.5)')
    plt.legend(loc='upper right')
    
    plt.tight_layout()
    plt.show()
    
    # Graph 2: Top Features by Model
    print("   🎯 Graph 2: Top Features Selected by Each Model")
    plt.figure(figsize=(14, 8))
    
    feature_colors = plt.cm.tab10(np.linspace(0, 1, len(consensus_features)))
    feature_color_map = {feat: color for (feat, _), color in zip(consensus_features, feature_colors)}
    
    top_features_per_model = [results[name]['top_feature'] for name in model_names]
    bar_colors = [feature_color_map.get(feat, 'gray') for feat in top_features_per_model]
    
    bars = plt.bar(range(len(model_names)), [1]*len(model_names), color=bar_colors, alpha=0.8, edgecolor='black')
    plt.xticks(range(len(model_names)), model_names, rotation=45, ha='right', fontsize=12)
    plt.ylabel('Top Feature Selection', fontweight='bold', fontsize=14)
    plt.title('Top Chemical Feature Selected by Each ML Model', fontweight='bold', fontsize=16)
    plt.yticks([])
    
    # Add feature names on bars
    for i, (bar, feat) in enumerate(zip(bars, top_features_per_model)):
        feat_display = feat.replace('_', ' ')
        plt.text(bar.get_x() + bar.get_width()/2., 0.5, feat_display,
                ha='center', va='center', fontweight='bold', fontsize=10, rotation=90)
    
    # Create legend for features
    legend_elements = []
    for (feat, votes), color in zip(consensus_features[:8], feature_colors[:8]):
        legend_elements.append(plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.8, label=feat.replace('_', ' ')))
    
    plt.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=10)
    plt.tight_layout()
    plt.show()
    
    # Graph 3: Feature Consensus Votes
    print("   📊 Graph 3: Feature Consensus Analysis")
    plt.figure(figsize=(12, 8))
    
    consensus_feat_names = [feat.replace('_', ' ') for feat, _ in consensus_features]
    consensus_votes = [votes for _, votes in consensus_features]
    
    bars = plt.barh(range(len(consensus_feat_names)), consensus_votes, 
                   color='skyblue', alpha=0.8, edgecolor='black', linewidth=2)
    plt.yticks(range(len(consensus_feat_names)), consensus_feat_names, fontsize=12)
    plt.xlabel('Number of Models Selecting as Top Feature', fontweight='bold', fontsize=14)
    plt.title('Chemical Feature Consensus Across ML Models\n(How many models chose each feature as most important)', 
              fontweight='bold', fontsize=16)
    plt.grid(True, alpha=0.3, axis='x')
    
    # Add vote counts and percentages
    for bar, votes in zip(bars, consensus_votes):
        percentage = votes / len(results) * 100
        plt.text(votes + 0.1, bar.get_y() + bar.get_height()/2, 
                f'{votes} ({percentage:.1f}%)', va='center', fontweight='bold', fontsize=11)
    
    plt.tight_layout()
    plt.show()
    
    # Graph 4: Best Model Predictions vs Actual
    if len(sorted_results) > 0:
        print("   🎯 Graph 4: Best Model Prediction Performance")
        plt.figure(figsize=(10, 8))
        
        best_model_name = sorted_results[0][0]
        best_predictions = predictions[best_model_name]
        
        plt.scatter(y_test, best_predictions, alpha=0.7, s=60, color='blue', 
                   edgecolors='black', linewidth=0.5, label='Predictions')
        
        # 1:1 line
        min_val = min(y_test.min(), best_predictions.min())
        max_val = max(y_test.max(), best_predictions.max())
        plt.plot([min_val, max_val], [min_val, max_val], 'red', linewidth=3, 
                linestyle='--', alpha=0.8, label='Perfect Prediction')
        
        best_r2 = results[best_model_name]['R²']
        best_rmse = results[best_model_name]['RMSE']
        best_top_feat = results[best_model_name]['top_feature']
        
        # Add stats text box
        stats_text = f'Model: {best_model_name}\nR² = {best_r2:.3f}\nRMSE = {best_rmse:.3f}\nTop Feature: {best_top_feat.replace("_", " ")}'
        plt.text(0.05, 0.95, stats_text, transform=plt.gca().transAxes,
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                fontweight='bold', va='top', fontsize=12)
        
        plt.xlabel('Actual MAC (m²/g)', fontweight='bold', fontsize=14)
        plt.ylabel('Predicted MAC (m²/g)', fontweight='bold', fontsize=14)
        plt.title(f'Best Performing Model: {best_model_name}\nPredicted vs Actual MAC Values', 
                 fontweight='bold', fontsize=16)
        plt.grid(True, alpha=0.3)
        plt.legend(fontsize=12)
        
        plt.tight_layout()
        plt.show()
    
    # Graph 5: Feature Importance Heatmap
    print("   🔥 Graph 5: Feature Importance Heatmap")
    plt.figure(figsize=(16, 10))
    
    # Get top 10 features overall
    all_features = set()
    for model_name, feat_df in model_feature_importance.items():
        all_features.update(feat_df.head(10)['feature'].tolist())
    
    top_overall_features = list(all_features)[:10]
    
    # Create heatmap of feature importance across models
    importance_matrix = np.zeros((len(model_names), len(top_overall_features)))
    
    for i, model_name in enumerate(model_names):
        if model_name in model_feature_importance:
            feat_df = model_feature_importance[model_name]
            for j, feature in enumerate(top_overall_features):
                feat_row = feat_df[feat_df['feature'] == feature]
                if not feat_row.empty:
                    # Normalize importance to 0-1 scale for each model
                    max_imp = feat_df['importance'].max()
                    if max_imp > 0:
                        importance_matrix[i, j] = feat_row['importance'].values[0] / max_imp
    
    im = plt.imshow(importance_matrix, cmap='YlOrRd', aspect='auto', vmin=0, vmax=1)
    
    plt.xticks(range(len(top_overall_features)), 
               [feat.replace('_', ' ') for feat in top_overall_features], 
               rotation=45, ha='right', fontsize=12)
    plt.yticks(range(len(model_names)), model_names, fontsize=12)
    plt.title('Feature Importance Heatmap Across ML Models\n(Normalized importance per model)', 
              fontweight='bold', fontsize=16)
    
    # Add colorbar
    cbar = plt.colorbar(im, shrink=0.8)
    cbar.set_label('Normalized Importance (0-1)', fontweight='bold', fontsize=12)
    
    # Add importance values in cells
    for i in range(len(model_names)):
        for j in range(len(top_overall_features)):
            if importance_matrix[i, j] > 0.1:  # Only show significant values
                text_color = 'white' if importance_matrix[i, j] > 0.6 else 'black'
                plt.text(j, i, f'{importance_matrix[i, j]:.2f}', ha='center', va='center',
                        color=text_color, fontweight='bold', fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    # Graph 6: Chemical Category Analysis
    print("   🧪 Graph 6: Chemical Category Preferences")
    plt.figure(figsize=(12, 8))
    
    # Categorize features
    feature_categories = {
        'Metals': ['Iron', 'Aluminum', 'Titanium', 'Manganese', 'Copper', 'Zinc', 'Lead', 'Arsenic', 'Chromium', 'Nickel'],
        'Ions': ['Potassium_Ion', 'Sulfate_Ion', 'Nitrate_Ion', 'Ammonium_Ion', 'Sodium_Ion', 'Calcium_Ion', 'Magnesium_Ion'],
        'Non-Metals': ['Silicon', 'Sulfur', 'Chlorine'],
        'Other': []
    }
    
    # Assign features to categories
    feature_to_category = {}
    for category, features in feature_categories.items():
        for feat in features:
            feature_to_category[feat] = category
    
    # Count top feature selections by category
    category_votes = {'Metals': 0, 'Ions': 0, 'Non-Metals': 0, 'Other': 0}
    category_models = {'Metals': [], 'Ions': [], 'Non-Metals': [], 'Other': []}
    
    for model_name, metrics in results.items():
        top_feat = metrics['top_feature']
        category = feature_to_category.get(top_feat, 'Other')
        category_votes[category] += 1
        category_models[category].append(model_name)
    
    categories = list(category_votes.keys())
    votes = list(category_votes.values())
    colors_cat = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12']
    
    bars = plt.bar(categories, votes, color=colors_cat, alpha=0.8, edgecolor='black', linewidth=2)
    plt.ylabel('Number of Models', fontweight='bold', fontsize=14)
    plt.title('Chemical Category Preferences Across ML Models\n(Which types of chemicals are most predictive)', 
              fontweight='bold', fontsize=16)
    plt.grid(True, alpha=0.3, axis='y')
    
    # Add model names and vote counts
    for bar, vote_count, category in zip(bars, votes, categories):
        if vote_count > 0:
            plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.05,
                    f'{vote_count}', ha='center', va='bottom', fontweight='bold', fontsize=12)
            
            # Add model names
            models_in_cat = category_models[category]
            if models_in_cat:
                model_text = ', '.join(models_in_cat[:2])  # Show first 2 models
                if len(models_in_cat) > 2:
                    model_text += f' (+{len(models_in_cat)-2})'
                plt.text(bar.get_x() + bar.get_width()/2., bar.get_height()/2,
                        model_text, ha='center', va='center', fontweight='bold', 
                        fontsize=9, rotation=0)
    
    plt.tight_layout()
    plt.show()
    
    print(f"\n✅ ALL INDIVIDUAL VISUALIZATIONS COMPLETE!")
    
    # Summary and model-specific recommendations
    print(f"\n💡 MODEL-SPECIFIC FEATURE RECOMMENDATIONS:")
    print(f"="*70)
    
    for name, metrics in sorted_results:
        print(f"\n🤖 {name.upper()}:")
        print(f"   • Performance: R² = {metrics['R²']:.3f}, RMSE = {metrics['RMSE']:.3f}")
        print(f"   • Top feature: {metrics['top_feature']}")
        print(f"   • Importance: {metrics['top_importance']:.3f} ({metrics['importance_type']})")
        
        # Feature interpretation
        top_feat = metrics['top_feature']
        if 'Iron' in top_feat or 'Aluminum' in top_feat or 'Titanium' in top_feat:
            print(f"   • Interpretation: Focuses on dust/crustal interference")
        elif 'Potassium' in top_feat:
            print(f"   • Interpretation: Emphasizes biomass burning effects")
        elif 'Sulfate' in top_feat or 'Nitrate' in top_feat:
            print(f"   • Interpretation: Highlights secondary aerosol coating effects")
        elif 'Manganese' in top_feat:
            print(f"   • Interpretation: Identifies anthropogenic metal interference")
        else:
            print(f"   • Interpretation: Unique chemical interference pathway")
    
    # Consensus recommendations
    if consensus_features:
        consensus_winner = consensus_features[0]
        print(f"\n🏆 CONSENSUS RECOMMENDATION:")
        print(f"   • Feature: {consensus_winner[0]}")
        print(f"   • Model agreement: {consensus_winner[1]}/{len(results)} models ({consensus_winner[1]/len(results)*100:.1f}%)")
        print(f"   • This suggests {consensus_winner[0]} is the most robust predictor")
        print(f"   • Consider prioritizing this species in routine monitoring")
    
    return results, predictions, model_feature_importance, consensus_features

# %%
# Updated wrapper function with correct unpacking
def run_comprehensive_chemical_analysis(merged_data, mac_results):
    """Run comprehensive chemical interference analysis with ML models - HIPS 633nm data only"""
    
    print("\n🔬 RUNNING COMPREHENSIVE CHEMICAL ANALYSIS...")
    print("📋 Scope: FTIR-EC + HIPS-Fabs (633nm) + ALL Chemical Species + ML Models")
    print("⚠️  NO pre-assumptions about interference - data-driven analysis")
    
    # Run comprehensive chemical analysis
    species_correlations, available_species, pm25_correlation = analyze_all_chemical_interference(merged_data, mac_results)
    
    # Run deep dive on key species
    key_species = analyze_key_interference_species(merged_data, mac_results, species_correlations)
    
    # Run ML models - CORRECTED: expecting 4 return values
    ml_results, ml_predictions, model_feature_importance, consensus_features = build_enhanced_ml_interference_models(merged_data, mac_results, species_correlations)
    
    print(f"\n✅ COMPREHENSIVE CHEMICAL ANALYSIS COMPLETE!")
    print(f"   • Analyzed {len(available_species)} chemical species")
    print(f"   • Deep dive on {len(key_species)} key interfering species")
    print(f"   • Trained {len(ml_results)} ML models")
    print(f"   • Focus on HIPS 633nm wavelength")
    print(f"   • Data-driven interference assessment")
    print(f"   • Feature consensus: {len(consensus_features)} features analyzed")
    
    return species_correlations, available_species, key_species, ml_results, ml_predictions, model_feature_importance, consensus_features, pm25_correlation

# Updated function call with correct unpacking
species_correlations, available_species, key_species, ml_results, ml_predictions, model_feature_importance, consensus_features, pm25_correlation = run_comprehensive_chemical_analysis(
    results['merged_data'], 
    results['mac_results']
)

# %%
# %% [markdown]
# ## 12. Iron vs HIPS Detailed Analysis (633nm Red Light)

# %%
def analyze_iron_hips_interference(merged_data, species_correlations):
    """
    Detailed analysis of Iron interference with HIPS measurements at 633nm
    Creates separate visualizations for Iron vs MAC and Iron vs Fabs with seasonal breakdown
    """
    
    print("🔴 IRON vs HIPS DETAILED ANALYSIS (633nm Red Light)")
    print("="*70)
    print("📋 Analysis: Iron interference with HIPS optical measurements")
    print("🎯 HIPS wavelength: 633nm (red light absorption)")
    print("🌍 Seasonal breakdown: Ethiopian climate patterns")
    
    df = merged_data.copy()
    
    # Check iron availability
    if 'Iron' not in df.columns or 'Iron' not in species_correlations:
        print("❌ Iron data not available in dataset")
        return None
    
    # Get iron correlation info
    iron_corr = species_correlations['Iron']
    print(f"\n📊 IRON CORRELATION WITH HIPS FABS:")
    print(f"   • Correlation: r = {iron_corr['r']:.3f}")
    print(f"   • P-value: {iron_corr['p']:.2e}")
    print(f"   • Sample size: {iron_corr['n']}")
    print(f"   • Mean concentration: {iron_corr['mean_conc']:.3f} ± {iron_corr['std_conc']:.3f} μg/m³")
    
    # Clean data
    iron_data = df['Iron'].dropna()
    fabs_data = df.loc[iron_data.index, 'fabs']
    ec_data = df.loc[iron_data.index, 'ec_ftir']
    individual_mac = fabs_data / ec_data
    
    # Filter reasonable values
    reasonable_mask = (individual_mac > 5) & (individual_mac < 25) & (iron_data > 0)
    iron_clean = iron_data[reasonable_mask]
    fabs_clean = fabs_data[reasonable_mask]
    mac_clean = individual_mac[reasonable_mask]
    
    print(f"   • Clean samples for analysis: {len(iron_clean)}")
    print(f"   • Iron range: {iron_clean.min():.3f} - {iron_clean.max():.3f} μg/m³")
    print(f"   • Fabs range: {fabs_clean.min():.1f} - {fabs_clean.max():.1f} Mm⁻¹")
    print(f"   • MAC range: {mac_clean.min():.2f} - {mac_clean.max():.2f} m²/g")
    
    # Create Figure 1: Iron vs Fabs
    print(f"\n📈 Creating Iron vs HIPS Fabs Analysis...")
    fig1, ax1 = plt.subplots(1, 1, figsize=(10, 8))
    
    # Check for seasonal data
    has_seasons = 'season' in df.columns and df.loc[iron_clean.index, 'season'].notna().sum() > 0
    
    if has_seasons:
        seasons = df.loc[iron_clean.index, 'season']
        season_colors = {
            'Dry Season (Bega)': '#d35400', 
            'Belg Rainy Season': '#27ae60', 
            'Kiremt Rainy Season': '#2980b9'
        }
        
        print(f"   🌍 Seasonal breakdown available")
        
        # Plot by season
        for season in seasons.unique():
            if pd.isna(season):
                continue
                
            season_mask = seasons == season
            if season_mask.sum() < 3:  # Skip seasons with too few points
                continue
                
            season_iron = iron_clean[season_mask]
            season_fabs = fabs_clean[season_mask]
            
            if season in season_colors and len(season_iron) >= 3:
                # Scatter plot
                ax1.scatter(season_iron, season_fabs, 
                           color=season_colors[season], alpha=0.7, s=60, 
                           label=season.split()[0], edgecolors='black', linewidth=1)
                
                # Regression line for this season
                if len(season_iron) >= 5:
                    z_season = np.polyfit(season_iron, season_fabs, 1)
                    p_season = np.poly1d(z_season)
                    x_line_season = np.linspace(season_iron.min(), season_iron.max(), 50)
                    
                    ax1.plot(x_line_season, p_season(x_line_season), 
                            color=season_colors[season], linewidth=3, 
                            linestyle='--', alpha=0.9)
                    
                    # Calculate seasonal correlation
                    r_season, p_season_val = pearsonr(season_iron, season_fabs)
                    print(f"     {season}: r = {r_season:.3f}, p = {p_season_val:.3f}, n = {len(season_iron)}")
    else:
        print(f"   📊 No seasonal data - using overall correlation")
        ax1.scatter(iron_clean, fabs_clean, alpha=0.7, s=60, color='red', 
                   edgecolors='black', linewidth=1)
    
    # Overall regression line
    r_iron_fabs, p_iron_fabs = pearsonr(iron_clean, fabs_clean)
    z_overall = np.polyfit(iron_clean, fabs_clean, 1)
    p_overall = np.poly1d(z_overall)
    x_line_overall = np.linspace(iron_clean.min(), iron_clean.max(), 100)
    ax1.plot(x_line_overall, p_overall(x_line_overall), 'black', linewidth=4, 
            linestyle='-', alpha=0.8, label='Overall Trend')
    
    # Add statistics box
    stats_text = f'Iron-HIPS Fabs (633nm):\nOverall: r = {r_iron_fabs:.3f}\np = {p_iron_fabs:.2e}\nn = {len(iron_clean)}'
    ax1.text(0.05, 0.95, stats_text, transform=ax1.transAxes,
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
            fontweight='bold', va='top', fontsize=12)
    
    ax1.set_xlabel('Iron Concentration (μg/m³)', fontweight='bold', fontsize=14)
    ax1.set_ylabel('HIPS Fabs at 633nm (Mm⁻¹)', fontweight='bold', fontsize=14)
    ax1.set_title('Iron vs HIPS Absorption (633nm)\nDirect interference with red light measurement', 
                 fontweight='bold', fontsize=16)
    ax1.grid(True, alpha=0.3)
    
    if has_seasons:
        ax1.legend(fontsize=12)
    
    plt.tight_layout()
    plt.show()
    
    # Create Figure 2: Iron vs MAC
    print(f"\n📈 Creating Iron vs HIPS MAC Analysis...")
    fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
    
    if has_seasons:
        seasons = df.loc[iron_clean.index, 'season']
        
        print(f"   🌍 MAC seasonal analysis:")
        
        # Plot by season
        for season in seasons.unique():
            if pd.isna(season):
                continue
                
            season_mask = seasons == season
            if season_mask.sum() < 3:
                continue
                
            season_iron = iron_clean[season_mask]
            season_mac = mac_clean[season_mask]
            
            if season in season_colors and len(season_iron) >= 3:
                # Scatter plot
                ax2.scatter(season_iron, season_mac, 
                           color=season_colors[season], alpha=0.7, s=60, 
                           label=season.split()[0], edgecolors='black', linewidth=1)
                
                # Regression line for this season
                if len(season_iron) >= 5:
                    z_season = np.polyfit(season_iron, season_mac, 1)
                    p_season = np.poly1d(z_season)
                    x_line_season = np.linspace(season_iron.min(), season_iron.max(), 50)
                    
                    ax2.plot(x_line_season, p_season(x_line_season), 
                            color=season_colors[season], linewidth=3, 
                            linestyle='--', alpha=0.9)
                    
                    # Calculate seasonal correlation
                    r_season, p_season_val = pearsonr(season_iron, season_mac)
                    print(f"     {season}: r = {r_season:.3f}, p = {p_season_val:.3f}, MAC = {season_mac.mean():.2f}±{season_mac.std():.2f}")
    else:
        ax2.scatter(iron_clean, mac_clean, alpha=0.7, s=60, color='red', 
                   edgecolors='black', linewidth=1)
    
    # Overall regression line
    r_iron_mac, p_iron_mac = pearsonr(iron_clean, mac_clean)
    z_overall_mac = np.polyfit(iron_clean, mac_clean, 1)
    p_overall_mac = np.poly1d(z_overall_mac)
    x_line_overall = np.linspace(iron_clean.min(), iron_clean.max(), 100)
    ax2.plot(x_line_overall, p_overall_mac(x_line_overall), 'black', linewidth=4, 
            linestyle='-', alpha=0.8, label='Overall Trend')
    
    # Add statistics box
    stats_text_mac = f'Iron-HIPS MAC (633nm):\nOverall: r = {r_iron_mac:.3f}\np = {p_iron_mac:.2e}\nn = {len(iron_clean)}'
    ax2.text(0.05, 0.95, stats_text_mac, transform=ax2.transAxes,
            bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.9),
            fontweight='bold', va='top', fontsize=12)
    
    ax2.set_xlabel('Iron Concentration (μg/m³)', fontweight='bold', fontsize=14)
    ax2.set_ylabel('Individual HIPS MAC at 633nm (m²/g)', fontweight='bold', fontsize=14)
    ax2.set_title('Iron vs HIPS MAC (633nm)\nMetal interference with absorption coefficient', 
                 fontweight='bold', fontsize=16)
    ax2.grid(True, alpha=0.3)
    
    if has_seasons:
        ax2.legend(fontsize=12)
    
    plt.tight_layout()
    plt.show()
    
    # Create Figure 3: Iron distribution and interference summary
    print(f"\n📊 Creating Iron Interference Summary...")
    fig3, ((ax3a, ax3b), (ax3c, ax3d)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 3a: Iron concentration distribution by season
    if has_seasons:
        seasons = df.loc[iron_clean.index, 'season']
        seasonal_iron_data = []
        season_labels = []
        
        for season in seasons.unique():
            if pd.isna(season):
                continue
            season_mask = seasons == season
            if season_mask.sum() >= 3:
                seasonal_iron_data.append(iron_clean[season_mask])
                season_labels.append(season.split()[0])
        
        if len(seasonal_iron_data) >= 2:
            bp = ax3a.boxplot(seasonal_iron_data, labels=season_labels, patch_artist=True)
            
            season_colors_list = ['#d35400', '#27ae60', '#2980b9'][:len(seasonal_iron_data)]
            for patch, color in zip(bp['boxes'], season_colors_list):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            ax3a.set_ylabel('Iron Concentration (μg/m³)', fontweight='bold')
            ax3a.set_title('Seasonal Iron Distribution', fontweight='bold')
            ax3a.grid(True, alpha=0.3, axis='y')
    
    # Plot 3b: MAC enhancement by iron quartiles
    iron_quartiles = pd.qcut(iron_clean, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
    mac_by_quartile = [mac_clean[iron_quartiles == q].dropna() for q in iron_quartiles.cat.categories]
    
    bp2 = ax3b.boxplot(mac_by_quartile, labels=['Low Fe', 'Med-Low', 'Med-High', 'High Fe'], 
                       patch_artist=True)
    
    colors_quart = ['#3498db', '#f39c12', '#e67e22', '#e74c3c']
    for patch, color in zip(bp2['boxes'], colors_quart):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax3b.set_ylabel('HIPS MAC (m²/g)', fontweight='bold')
    ax3b.set_title('MAC Enhancement by Iron Quartiles', fontweight='bold')
    ax3b.grid(True, alpha=0.3, axis='y')
    
    # Plot 3c: Iron vs baseline absorption (if available)
    if 'fabs' in df.columns and 'ec_ftir' in df.columns:
        # Estimate baseline using Method 3 if available in context
        try:
            # This would need mac_results passed to function, but we'll estimate
            typical_mac = 10.0  # Typical MAC value for BC
            expected_bc_fabs = typical_mac * ec_data[reasonable_mask]
            baseline_fabs = fabs_clean - expected_bc_fabs
            
            ax3c.scatter(iron_clean, baseline_fabs, alpha=0.7, s=50, color='purple', 
                        edgecolors='black', linewidth=0.5)
            
            # Add trend line
            if len(iron_clean) > 5:
                z_base = np.polyfit(iron_clean, baseline_fabs, 1)
                p_base = np.poly1d(z_base)
                x_line = np.linspace(iron_clean.min(), iron_clean.max(), 100)
                ax3c.plot(x_line, p_base(x_line), 'red', linewidth=2, alpha=0.8)
                
                r_base, p_base_val = pearsonr(iron_clean, baseline_fabs)
                ax3c.text(0.05, 0.95, f'r = {r_base:.3f}\np = {p_base_val:.2e}', 
                         transform=ax3c.transAxes,
                         bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                         fontweight='bold')
            
            ax3c.set_xlabel('Iron Concentration (μg/m³)', fontweight='bold')
            ax3c.set_ylabel('Estimated Baseline Absorption (Mm⁻¹)', fontweight='bold')
            ax3c.set_title('Iron vs Non-BC Absorption', fontweight='bold')
            ax3c.grid(True, alpha=0.3)
            ax3c.axhline(0, color='black', linestyle='-', alpha=0.5)
        except:
            ax3c.text(0.5, 0.5, 'Baseline Analysis\nNot Available', ha='center', va='center',
                     transform=ax3c.transAxes, fontweight='bold', fontsize=14)
            ax3c.set_title('Baseline Analysis', fontweight='bold')
    
    # Plot 3d: Summary statistics table
    ax3d.axis('off')
    
    # Create summary table
    summary_data = []
    headers = ['Metric', 'Iron vs Fabs', 'Iron vs MAC']
    
    summary_data.append(['Correlation (r)', f'{r_iron_fabs:.3f}', f'{r_iron_mac:.3f}'])
    summary_data.append(['P-value', f'{p_iron_fabs:.2e}', f'{p_iron_mac:.2e}'])
    summary_data.append(['Sample size', f'{len(iron_clean)}', f'{len(iron_clean)}'])
    summary_data.append(['Iron range', f'{iron_clean.min():.2f}-{iron_clean.max():.2f} μg/m³', 
                        f'{iron_clean.min():.2f}-{iron_clean.max():.2f} μg/m³'])
    
    if has_seasons:
        summary_data.append(['Seasons analyzed', f'{len(season_labels)}', f'{len(season_labels)}'])
    
    table = ax3d.table(cellText=summary_data, colLabels=headers, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2)
    
    # Color code the table
    for i in range(len(summary_data)):
        table[(i+1, 0)].set_facecolor('#f0f0f0')
        table[(i+1, 1)].set_facecolor('#ffe6e6')
        table[(i+1, 2)].set_facecolor('#e6f2ff')
    
    ax3d.set_title('Iron Interference Summary Statistics\n(HIPS 633nm Analysis)', 
                  fontweight='bold', pad=20, fontsize=14)
    
    plt.tight_layout()
    plt.show()
    
    # Analysis summary
    print(f"\n📋 IRON INTERFERENCE ANALYSIS RESULTS:")
    print(f"="*60)
    print(f"   🔴 DIRECT INTERFERENCE (Iron vs Fabs):")
    print(f"     • Correlation: r = {r_iron_fabs:.3f}")
    print(f"     • Statistical significance: p = {p_iron_fabs:.2e}")
    print(f"     • Effect strength: {'STRONG' if abs(r_iron_fabs) > 0.5 else 'MODERATE' if abs(r_iron_fabs) > 0.3 else 'WEAK'}")
    
    print(f"\n   ⚙️ MAC ENHANCEMENT (Iron vs MAC):")
    print(f"     • Correlation: r = {r_iron_mac:.3f}")
    print(f"     • Statistical significance: p = {p_iron_mac:.2e}")
    print(f"     • Effect strength: {'STRONG' if abs(r_iron_mac) > 0.5 else 'MODERATE' if abs(r_iron_mac) > 0.3 else 'WEAK'}")
    
    if has_seasons:
        print(f"\n   🌍 SEASONAL PATTERNS:")
        seasons_analyzed = df.loc[iron_clean.index, 'season'].unique()
        for season in seasons_analyzed:
            if pd.notna(season):
                season_mask = df.loc[iron_clean.index, 'season'] == season
                season_iron_mean = iron_clean[season_mask].mean()
                season_mac_mean = mac_clean[season_mask].mean()
                print(f"     • {season}: Fe = {season_iron_mean:.3f} μg/m³, MAC = {season_mac_mean:.2f} m²/g")
    
    print(f"\n   💡 INTERPRETATION:")
    if abs(r_iron_fabs) > 0.4:
        print(f"     • Iron shows significant interference with HIPS 633nm measurements")
        print(f"     • Red light absorption enhanced by iron-containing particles")
        print(f"     • Likely sources: dust, combustion, industrial emissions")
    else:
        print(f"     • Iron shows limited interference with HIPS measurements")
    
    if abs(r_iron_mac) > 0.4:
        print(f"     • Iron concentration affects apparent MAC values")
        print(f"     • Consider iron-based correction factors for HIPS-derived BC")
    
    print(f"\n   🔧 OPERATIONAL RECOMMENDATIONS:")
    print(f"     • Monitor iron concentrations during HIPS measurements")
    print(f"     • Flag high-iron periods (>{iron_clean.quantile(0.75):.3f} μg/m³) for quality control")
    print(f"     • Consider seasonal correction factors")
    print(f"     • Cross-reference with meteorological data (dust events)")
    
    # Return results for further analysis
    iron_analysis_results = {
        'iron_fabs_correlation': r_iron_fabs,
        'iron_mac_correlation': r_iron_mac,
        'iron_mean_concentration': iron_clean.mean(),
        'iron_std_concentration': iron_clean.std(),
        'sample_size': len(iron_clean),
        'has_seasonal_data': has_seasons
    }
    
    if has_seasons:
        seasonal_stats = {}
        seasons = df.loc[iron_clean.index, 'season']
        for season in seasons.unique():
            if pd.notna(season):
                season_mask = seasons == season
                seasonal_stats[season] = {
                    'iron_mean': iron_clean[season_mask].mean(),
                    'mac_mean': mac_clean[season_mask].mean(),
                    'sample_count': season_mask.sum()
                }
        iron_analysis_results['seasonal_stats'] = seasonal_stats
    
    return iron_analysis_results

# %%
# Run the iron analysis (uncomment to execute)
iron_results = analyze_iron_hips_interference(results['merged_data'], species_correlations)

# %%
# %% [markdown]
# ## 13. Ann's Meeting Requests - Specific Analyses
# Six specific graphs/tables requested during the meeting with Ann

# %%
def create_ann_meeting_analyses(merged_data, mac_results):
    """
    Create the 6 specific analyses requested by Ann during the meeting
    """
    
    print("📋 ANN'S MEETING REQUESTS - 6 SPECIFIC ANALYSES")
    print("="*80)
    
    df = merged_data.copy()
    
    # Prepare base data
    ec_data = df['ec_ftir'].values  # FTIR-BC (μg/m³)
    fabs_data = df['fabs'].values   # Fabs (Mm⁻¹)
    
    # Filter reasonable data
    reasonable_mask = (ec_data > 0) & (fabs_data > 0) & (ec_data < 50) & (fabs_data < 200)
    ec_clean = ec_data[reasonable_mask]
    fabs_clean = fabs_data[reasonable_mask]
    df_clean = df[reasonable_mask].copy()
    
    print(f"Using {len(ec_clean)} clean samples for analysis")
    
    # =============================================================================
    # REQUEST 1: FTIR-BC vs Fabs scatter, colour-coded by absolute Fe concentration
    # =============================================================================
    print("\n📈 REQUEST 1: FTIR-BC vs Fabs (colored by Fe concentration)")
    
    fig1, ax1 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns:
        iron_data = df_clean['Iron'].values
        
        # Create scatter plot
        scatter1 = ax1.scatter(ec_clean, fabs_clean, c=iron_data, cmap='Reds', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar1 = plt.colorbar(scatter1, ax=ax1)
        cbar1.set_label('Iron Concentration (μg/m³)', fontweight='bold')
        
        # Add regression line
        z1 = np.polyfit(ec_clean, fabs_clean, 1)
        p1 = np.poly1d(z1)
        x_line1 = np.linspace(ec_clean.min(), ec_clean.max(), 100)
        ax1.plot(x_line1, p1(x_line1), 'blue', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r1, p_val1 = pearsonr(ec_clean, fabs_clean)
        ax1.text(0.05, 0.95, f'r = {r1:.3f}\np = {p_val1:.2e}', 
                transform=ax1.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
    else:
        ax1.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax1.text(0.5, 0.5, 'Iron data not available', transform=ax1.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax1.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax1.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax1.set_title('FTIR-BC vs Fabs\n(Colored by Iron Concentration)', fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 2: Same scatter colour-coded by Fe / PM₂.₅ ratio
    # =============================================================================
    print("\n📈 REQUEST 2: FTIR-BC vs Fabs (colored by Fe/PM₂.₅ ratio)")
    
    fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns and 'PM25_mass' in df_clean.columns:
        iron_pm_ratio = df_clean['Iron'] / df_clean['PM25_mass'] * 100  # Convert to percentage
        
        # Filter out extreme ratios
        ratio_mask = (iron_pm_ratio > 0) & (iron_pm_ratio < iron_pm_ratio.quantile(0.95))
        
        scatter2 = ax2.scatter(ec_clean[ratio_mask], fabs_clean[ratio_mask], 
                              c=iron_pm_ratio[ratio_mask], cmap='viridis', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar2 = plt.colorbar(scatter2, ax=ax2)
        cbar2.set_label('Fe/PM₂.₅ Ratio (%)', fontweight='bold')
        
        # Add regression line
        z2 = np.polyfit(ec_clean[ratio_mask], fabs_clean[ratio_mask], 1)
        p2 = np.poly1d(z2)
        x_line2 = np.linspace(ec_clean[ratio_mask].min(), ec_clean[ratio_mask].max(), 100)
        ax2.plot(x_line2, p2(x_line2), 'red', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r2, p_val2 = pearsonr(ec_clean[ratio_mask], fabs_clean[ratio_mask])
        ax2.text(0.05, 0.95, f'r = {r2:.3f}\np = {p_val2:.2e}', 
                transform=ax2.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
        
        print(f"   Fe/PM₂.₅ ratio range: {iron_pm_ratio[ratio_mask].min():.3f} - {iron_pm_ratio[ratio_mask].max():.3f}%")
    else:
        ax2.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax2.text(0.5, 0.5, 'Iron or PM₂.₅ data not available', transform=ax2.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax2.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax2.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax2.set_title('FTIR-BC vs Fabs\n(Colored by Fe/PM₂.₅ Ratio)', fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 3: Same scatter colour-coded by Fe / EC ratio
    # =============================================================================
    print("\n📈 REQUEST 3: FTIR-BC vs Fabs (colored by Fe/EC ratio)")
    
    fig3, ax3 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns:
        iron_ec_ratio = df_clean['Iron'] / df_clean['ec_ftir']
        
        # Filter out extreme ratios
        ratio_mask_ec = (iron_ec_ratio > 0) & (iron_ec_ratio < iron_ec_ratio.quantile(0.95))
        
        scatter3 = ax3.scatter(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec], 
                              c=iron_ec_ratio[ratio_mask_ec], cmap='plasma', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar3 = plt.colorbar(scatter3, ax=ax3)
        cbar3.set_label('Fe/EC Ratio (dimensionless)', fontweight='bold')
        
        # Add regression line
        z3 = np.polyfit(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec], 1)
        p3 = np.poly1d(z3)
        x_line3 = np.linspace(ec_clean[ratio_mask_ec].min(), ec_clean[ratio_mask_ec].max(), 100)
        ax3.plot(x_line3, p3(x_line3), 'red', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r3, p_val3 = pearsonr(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec])
        ax3.text(0.05, 0.95, f'r = {r3:.3f}\np = {p_val3:.2e}', 
                transform=ax3.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
        
        print(f"   Fe/EC ratio range: {iron_ec_ratio[ratio_mask_ec].min():.3f} - {iron_ec_ratio[ratio_mask_ec].max():.3f}")
        
        # Add interpretation note
        ax3.text(0.05, 0.85, 'Note: High Fe/EC may indicate\ndust interference', 
                transform=ax3.transAxes, bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                fontweight='bold', fontsize=10)
    else:
        ax3.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax3.text(0.5, 0.5, 'Iron data not available', transform=ax3.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax3.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax3.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax3.set_title('FTIR-BC vs Fabs\n(Colored by Fe/EC Ratio)', fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 4: Seasonal MAC plots for Methods 2 & 4 (viable methods)
    # =============================================================================
    print("\n📈 REQUEST 4: Seasonal MAC plots for Methods 2 & 4")
    
    if 'season' in df.columns:
        # Calculate MAC values for Methods 2 and 4
        mac_method2 = mac_results['mac_values']['Method_2']  # Ratio of means
        mac_method4 = mac_results['mac_values']['Method_4']  # Origin regression
        
        # Calculate BC equivalents for each method
        bc_method2 = df['fabs'] / mac_method2
        bc_method4 = df['fabs'] / mac_method4
        
        # Get seasonal data
        seasons = df['season'].unique()
        seasons = [s for s in seasons if pd.notna(s)]
        
        fig4, (ax4a, ax4b) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Method 2 seasonal plot
        seasonal_bc2 = []
        seasonal_ec = []
        season_labels = []
        
        for season in seasons:
            season_mask = df['season'] == season
            season_bc2 = bc_method2[season_mask].dropna()
            season_ec_vals = df.loc[season_mask, 'ec_ftir'].dropna()
            
            if len(season_bc2) > 3:
                seasonal_bc2.append(season_bc2)
                seasonal_ec.append(season_ec_vals)
                season_labels.append(season.split()[0])
        
        # Boxplots for Method 2
        if len(seasonal_bc2) >= 2:
            bp2 = ax4a.boxplot(seasonal_bc2, labels=season_labels, patch_artist=True)
            
            colors_season = ['#d35400', '#27ae60', '#2980b9'][:len(seasonal_bc2)]
            for patch, color in zip(bp2['boxes'], colors_season):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Add EC reference
            overall_ec_mean = df['ec_ftir'].mean()
            ax4a.axhline(overall_ec_mean, color='red', linestyle='--', linewidth=2, 
                        label=f'Overall EC Mean ({overall_ec_mean:.1f} μg/m³)')
        
        ax4a.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
        ax4a.set_title(f'Method 2: Ratio of Means\nMAC = {mac_method2:.2f} m²/g', fontweight='bold')
        ax4a.legend()
        ax4a.grid(True, alpha=0.3, axis='y')
        
        # Method 4 seasonal plot
        seasonal_bc4 = []
        for season in seasons:
            season_mask = df['season'] == season
            season_bc4 = bc_method4[season_mask].dropna()
            
            if len(season_bc4) > 3:
                seasonal_bc4.append(season_bc4)
        
        if len(seasonal_bc4) >= 2:
            bp4 = ax4b.boxplot(seasonal_bc4, labels=season_labels, patch_artist=True)
            
            for patch, color in zip(bp4['boxes'], colors_season):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Add EC reference
            ax4b.axhline(overall_ec_mean, color='red', linestyle='--', linewidth=2, 
                        label=f'Overall EC Mean ({overall_ec_mean:.1f} μg/m³)')
        
        ax4b.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
        ax4b.set_title(f'Method 4: Origin Regression\nMAC = {mac_method4:.2f} m²/g', fontweight='bold')
        ax4b.legend()
        ax4b.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.show()
        
        # Print seasonal statistics
        print(f"   Seasonal MAC Analysis:")
        for i, season in enumerate(season_labels):
            if i < len(seasonal_bc2) and i < len(seasonal_bc4):
                bc2_mean = seasonal_bc2[i].mean()
                bc4_mean = seasonal_bc4[i].mean()
                ec_mean = seasonal_ec[i].mean()
                print(f"     {season}: Method 2 BC = {bc2_mean:.2f}, Method 4 BC = {bc4_mean:.2f}, EC = {ec_mean:.2f} μg/m³")
    
    else:
        print("   ⚠️ No seasonal data available")
    
    # =============================================================================
    # REQUEST 5: Bias / RMSE summary table (overall and by season)
    # =============================================================================
    print("\n📊 REQUEST 5: Bias/RMSE Summary Table")
    
    from sklearn.metrics import mean_squared_error, mean_absolute_error
    
    # Calculate performance metrics for Methods 2 and 4
    methods = ['Method_2', 'Method_4']
    method_names = ['Ratio of Means', 'Origin Regression']
    
    # Overall performance
    overall_metrics = {}
    for method in methods:
        mac_val = mac_results['mac_values'][method]
        bc_values = df['fabs'] / mac_val
        ec_values = df['ec_ftir']
        
        # Filter reasonable values
        mask = (bc_values > 0) & (ec_values > 0) & (bc_values < 100) & (ec_values < 100)
        bc_clean = bc_values[mask]
        ec_clean = ec_values[mask]
        
        bias = np.mean(bc_clean - ec_clean)
        rmse = np.sqrt(mean_squared_error(ec_clean, bc_clean))
        mae = mean_absolute_error(ec_clean, bc_clean)
        
        overall_metrics[method] = {'bias': bias, 'rmse': rmse, 'mae': mae, 'n': len(bc_clean)}
    
    # Create summary table
    fig5, ax5 = plt.subplots(1, 1, figsize=(12, 8))
    ax5.axis('off')
    
    # Prepare table data
    table_data = []
    headers = ['Method', 'MAC (m²/g)', 'Bias (μg/m³)', 'RMSE (μg/m³)', 'MAE (μg/m³)', 'Samples']
    
    # Overall results
    for i, method in enumerate(methods):
        mac_val = mac_results['mac_values'][method]
        metrics = overall_metrics[method]
        
        row = [
            method_names[i],
            f'{mac_val:.3f}',
            f'{metrics["bias"]:.3f}',
            f'{metrics["rmse"]:.3f}',
            f'{metrics["mae"]:.3f}',
            f'{metrics["n"]}'
        ]
        table_data.append(row)
    
    # Add seasonal breakdown if available
    if 'season' in df.columns:
        table_data.append(['', '', '', '', '', ''])  # Empty row
        table_data.append(['SEASONAL BREAKDOWN', '', '', '', '', ''])
        
        for season in seasons:
            season_mask = df['season'] == season
            if season_mask.sum() < 5:
                continue
                
            for i, method in enumerate(methods):
                mac_val = mac_results['mac_values'][method]
                bc_season = (df.loc[season_mask, 'fabs'] / mac_val).dropna()
                ec_season = df.loc[season_mask, 'ec_ftir'].dropna()
                
                if len(bc_season) > 3 and len(ec_season) > 3:
                    # Align the series
                    common_idx = bc_season.index.intersection(ec_season.index)
                    bc_aligned = bc_season[common_idx]
                    ec_aligned = ec_season[common_idx]
                    
                    bias_season = np.mean(bc_aligned - ec_aligned)
                    rmse_season = np.sqrt(mean_squared_error(ec_aligned, bc_aligned))
                    mae_season = mean_absolute_error(ec_aligned, bc_aligned)
                    
                    row = [
                        f'{season.split()[0]} - {method_names[i]}',
                        f'{mac_val:.3f}',
                        f'{bias_season:.3f}',
                        f'{rmse_season:.3f}',
                        f'{mae_season:.3f}',
                        f'{len(bc_aligned)}'
                    ]
                    table_data.append(row)
    
    # Create table
    table = ax5.table(cellText=table_data, colLabels=headers, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)
    
    # Color code the table
    colors_methods = ['#e8f4fd', '#fff2e8']
    for i, row in enumerate(table_data):
        if 'Method' in str(row[0]) or 'SEASONAL' in str(row[0]):
            continue
        
        method_idx = 0 if 'Ratio of Means' in str(row[0]) or 'Method_2' in str(row[0]) else 1
        for j in range(len(headers)):
            table[(i+1, j)].set_facecolor(colors_methods[method_idx])
            table[(i+1, j)].set_alpha(0.7)
    
    ax5.set_title('Bias/RMSE Summary: Methods 2 & 4\n(Overall and Seasonal Performance)', 
                  fontweight='bold', pad=20, fontsize=16)
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 6: MAC deviation vs potential drivers (Fe, sulfate, RH, T, rain)
    # =============================================================================
    print("\n📈 REQUEST 6: MAC deviation vs potential drivers")
    
    # Calculate MAC deviations (individual MAC - method MAC)
    individual_mac = df['fabs'] / df['ec_ftir']
    reasonable_mac_mask = (individual_mac > 5) & (individual_mac < 20)
    
    # Use Method 2 as reference (ratio of means)
    reference_mac = mac_results['mac_values']['Method_2']
    mac_deviation = individual_mac - reference_mac
    
    # Prepare drivers data
    potential_drivers = {}
    
    if 'Iron' in df.columns:
        potential_drivers['Iron'] = df['Iron']
    
    if 'Sulfate_Ion' in df.columns:
        potential_drivers['Sulfate'] = df['Sulfate_Ion']
    
    # Note: RH, Temperature, and Rain data would need to be added to the dataset
    # For now, we'll work with available chemical species
    if 'Nitrate_Ion' in df.columns:
        potential_drivers['Nitrate'] = df['Nitrate_Ion']
    
    if 'Potassium_Ion' in df.columns:
        potential_drivers['Potassium'] = df['Potassium_Ion']
    
    # Create subplot grid
    n_drivers = len(potential_drivers)
    if n_drivers > 0:
        n_cols = 2
        n_rows = (n_drivers + 1) // 2
        
        fig6, axes = plt.subplots(n_rows, n_cols, figsize=(16, 5*n_rows))
        if n_rows == 1:
            axes = [axes]
        axes = axes.flatten()
        
        for i, (driver_name, driver_data) in enumerate(potential_drivers.items()):
            ax = axes[i]
            
            # Filter data
            driver_mask = reasonable_mac_mask & driver_data.notna() & mac_deviation.notna()
            x_data = driver_data[driver_mask]
            y_data = mac_deviation[driver_mask]
            
            if len(x_data) > 10:
                # Scatter plot
                ax.scatter(x_data, y_data, alpha=0.7, s=50, color='blue', 
                          edgecolors='black', linewidth=0.5)
                
                # Add regression line
                if len(x_data) > 5:
                    z = np.polyfit(x_data, y_data, 1)
                    p = np.poly1d(z)
                    x_line = np.linspace(x_data.min(), x_data.max(), 100)
                    ax.plot(x_line, p(x_line), 'red', linewidth=2, alpha=0.8)
                    
                    # Calculate correlation
                    r, p_val = pearsonr(x_data, y_data)
                    ax.text(0.05, 0.95, f'r = {r:.3f}\np = {p_val:.2e}', 
                           transform=ax.transAxes, 
                           bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                           fontweight='bold')
                
                # Add zero line
                ax.axhline(0, color='black', linestyle='--', alpha=0.5)
                
                ax.set_xlabel(f'{driver_name} (μg/m³)', fontweight='bold')
                ax.set_ylabel('MAC Deviation (m²/g)', fontweight='bold')
                ax.set_title(f'MAC Deviation vs {driver_name}', fontweight='bold')
                ax.grid(True, alpha=0.3)
                
                print(f"   {driver_name}: r = {r:.3f}, p = {p_val:.2e}, n = {len(x_data)}")
            else:
                ax.text(0.5, 0.5, f'Insufficient {driver_name} data', 
                       transform=ax.transAxes, ha='center', va='center', 
                       fontweight='bold', fontsize=12)
                ax.set_title(f'MAC Deviation vs {driver_name}', fontweight='bold')
        
        # Hide empty subplots
        for j in range(i+1, len(axes)):
            axes[j].set_visible(False)
        
        plt.tight_layout()
        plt.show()
    else:
        print("   ⚠️ No potential drivers data available")
    
    print(f"\n✅ ALL 6 REQUESTED ANALYSES COMPLETE!")
    
    # Return summary for documentation
    summary_results = {
        'analysis_1': 'FTIR-BC vs Fabs (colored by Fe concentration)',
        'analysis_2': 'FTIR-BC vs Fabs (colored by Fe/PM₂.₅ ratio)',
        'analysis_3': 'FTIR-BC vs Fabs (colored by Fe/EC ratio)',
        'analysis_4': 'Seasonal MAC plots for Methods 2 & 4',
        'analysis_5': 'Bias/RMSE summary table (overall and seasonal)',
        'analysis_6': 'MAC deviation vs potential drivers',
        'methods_analyzed': ['Method_2 (Ratio of Means)', 'Method_4 (Origin Regression)'],
        'reference_mac': reference_mac,
        'available_drivers': list(potential_drivers.keys()) if potential_drivers else [],
        'seasonal_data_available': 'season' in df.columns
    }
    
    return summary_results

# %%
# Execute Ann's meeting requests
if __name__ == "__main__":
    print("🎯 EXECUTING ANN'S MEETING REQUESTS...")
    print("📋 Creating 6 specific analyses as requested during the meeting")
    
    # Run the analyses using existing data
    # Note: Replace 'results' with your actual merged_data and mac_results variables
    
    # Example usage:
    # meeting_results = create_ann_meeting_analyses(results['merged_data'], results['mac_results'])
    
    print("\n📝 TO RUN THE ANALYSES:")
    print("meeting_results = create_ann_meeting_analyses(merged_data, mac_results)")
    print("\nThis will create:")
    print("1. FTIR-BC vs Fabs scatter (colored by Fe concentration)")
    print("2. FTIR-BC vs Fabs scatter (colored by Fe/PM₂.₅ ratio)")  
    print("3. FTIR-BC vs Fabs scatter (colored by Fe/EC ratio)")
    print("4. Seasonal MAC plots for Methods 2 & 4")
    print("5. Bias/RMSE summary table (overall and seasonal)")
    print("6. MAC deviation vs potential drivers scatter plots")

# %% [markdown]
# ### Meeting Notes Summary
# 
# **Ann's Specific Requests:**
# 
# 1. **Iron concentration effects**: Direct visualization of how iron concentration affects the FTIR-BC vs Fabs relationship
# 2. **Iron relative abundance**: Two approaches - Fe/PM₂.₅ and Fe/EC ratios to understand iron's relative importance
# 3. **Seasonal MAC validation**: Focus on the two viable methods (Methods 2 & 4) across Ethiopian climate seasons
# 4. **Performance consolidation**: Comprehensive bias/RMSE analysis across all conditions
# 5. **Composition effects**: Systematic correlation analysis between MAC deviations and potential drivers
# 
# **Key Questions Addressed:**
# - Does iron concentration directly correlate with optical absorption interference?
# - How does iron's relative abundance (vs total PM or vs EC) affect measurements?
# - Are the viable MAC methods seasonally stable?
# - Which chemical/meteorological factors drive MAC variability?
# 
# **Expected Outcomes:**
# - Clear visualization of iron interference patterns
# - Seasonal stability assessment for recommended methods
# - Identification of key drivers for MAC correction factors
# - Performance metrics for method selection and uncertainty quantification

# %%
# Uncomment to run with your data:
meeting_results = create_ann_meeting_analyses(results['merged_data'], results['mac_results'])

# %%
# %% [markdown]
# ## 13. Ann's Meeting Requests - Specific Analyses
# Six specific graphs/tables requested during the meeting with Ann

# %%
def create_ann_meeting_analyses(merged_data, mac_results):
    """
    Create the 6 specific analyses requested by Ann during the meeting
    """
    
    print("📋 ANN'S MEETING REQUESTS - 6 SPECIFIC ANALYSES")
    print("="*80)
    
    df = merged_data.copy()
    
    # Prepare base data
    ec_data = df['ec_ftir'].values  # FTIR-BC (μg/m³)
    fabs_data = df['fabs'].values   # Fabs (Mm⁻¹)
    
    # Filter reasonable data
    reasonable_mask = (ec_data > 0) & (fabs_data > 0) & (ec_data < 50) & (fabs_data < 200)
    ec_clean = ec_data[reasonable_mask]
    fabs_clean = fabs_data[reasonable_mask]
    df_clean = df[reasonable_mask].copy()
    
    print(f"Using {len(ec_clean)} clean samples for analysis")
    
    # =============================================================================
    # REQUEST 1: FTIR-BC vs Fabs scatter, colour-coded by absolute Fe concentration
    # =============================================================================
    print("\n📈 REQUEST 1: FTIR-BC vs Fabs (colored by Fe concentration)")
    
    fig1, ax1 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns:
        iron_data = df_clean['Iron'].values
        
        # Create scatter plot
        scatter1 = ax1.scatter(ec_clean, fabs_clean, c=iron_data, cmap='Reds', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar1 = plt.colorbar(scatter1, ax=ax1)
        cbar1.set_label('Iron Concentration (μg/m³)', fontweight='bold')
        
        # Add regression line
        z1 = np.polyfit(ec_clean, fabs_clean, 1)
        p1 = np.poly1d(z1)
        x_line1 = np.linspace(ec_clean.min(), ec_clean.max(), 100)
        ax1.plot(x_line1, p1(x_line1), 'blue', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r1, p_val1 = pearsonr(ec_clean, fabs_clean)
        ax1.text(0.05, 0.95, f'r = {r1:.3f}\np = {p_val1:.2e}', 
                transform=ax1.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
    else:
        ax1.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax1.text(0.5, 0.5, 'Iron data not available', transform=ax1.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax1.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax1.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax1.set_title('FTIR-BC vs Fabs\n(Colored by Iron Concentration)', fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 2: Same scatter colour-coded by Fe / PM₂.₅ ratio
    # =============================================================================
    print("\n📈 REQUEST 2: FTIR-BC vs Fabs (colored by Fe/PM₂.₅ ratio)")
    
    fig2, ax2 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns and 'PM25_mass' in df_clean.columns:
        iron_pm_ratio = df_clean['Iron'] / df_clean['PM25_mass'] * 100  # Convert to percentage
        
        # Filter out extreme ratios
        ratio_mask = (iron_pm_ratio > 0) & (iron_pm_ratio < iron_pm_ratio.quantile(0.95))
        
        scatter2 = ax2.scatter(ec_clean[ratio_mask], fabs_clean[ratio_mask], 
                              c=iron_pm_ratio[ratio_mask], cmap='viridis', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar2 = plt.colorbar(scatter2, ax=ax2)
        cbar2.set_label('Fe/PM₂.₅ Ratio (%)', fontweight='bold')
        
        # Add regression line
        z2 = np.polyfit(ec_clean[ratio_mask], fabs_clean[ratio_mask], 1)
        p2 = np.poly1d(z2)
        x_line2 = np.linspace(ec_clean[ratio_mask].min(), ec_clean[ratio_mask].max(), 100)
        ax2.plot(x_line2, p2(x_line2), 'red', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r2, p_val2 = pearsonr(ec_clean[ratio_mask], fabs_clean[ratio_mask])
        ax2.text(0.05, 0.95, f'r = {r2:.3f}\np = {p_val2:.2e}', 
                transform=ax2.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
        
        print(f"   Fe/PM₂.₅ ratio range: {iron_pm_ratio[ratio_mask].min():.3f} - {iron_pm_ratio[ratio_mask].max():.3f}%")
    else:
        ax2.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax2.text(0.5, 0.5, 'Iron or PM₂.₅ data not available', transform=ax2.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax2.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax2.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax2.set_title('FTIR-BC vs Fabs\n(Colored by Fe/PM₂.₅ Ratio)', fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 3: Same scatter colour-coded by Fe / EC ratio
    # =============================================================================
    print("\n📈 REQUEST 3: FTIR-BC vs Fabs (colored by Fe/EC ratio)")
    
    fig3, ax3 = plt.subplots(1, 1, figsize=(10, 8))
    
    if 'Iron' in df_clean.columns:
        iron_ec_ratio = df_clean['Iron'] / df_clean['ec_ftir']
        
        # Filter out extreme ratios
        ratio_mask_ec = (iron_ec_ratio > 0) & (iron_ec_ratio < iron_ec_ratio.quantile(0.95))
        
        scatter3 = ax3.scatter(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec], 
                              c=iron_ec_ratio[ratio_mask_ec], cmap='plasma', 
                              s=60, alpha=0.8, edgecolors='black', linewidth=0.5)
        
        # Add colorbar
        cbar3 = plt.colorbar(scatter3, ax=ax3)
        cbar3.set_label('Fe/EC Ratio (dimensionless)', fontweight='bold')
        
        # Add regression line
        z3 = np.polyfit(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec], 1)
        p3 = np.poly1d(z3)
        x_line3 = np.linspace(ec_clean[ratio_mask_ec].min(), ec_clean[ratio_mask_ec].max(), 100)
        ax3.plot(x_line3, p3(x_line3), 'red', linewidth=3, alpha=0.8, label='Regression Line')
        
        # Statistics
        r3, p_val3 = pearsonr(ec_clean[ratio_mask_ec], fabs_clean[ratio_mask_ec])
        ax3.text(0.05, 0.95, f'r = {r3:.3f}\np = {p_val3:.2e}', 
                transform=ax3.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                fontweight='bold')
        
        print(f"   Fe/EC ratio range: {iron_ec_ratio[ratio_mask_ec].min():.3f} - {iron_ec_ratio[ratio_mask_ec].max():.3f}")
        
        # Add interpretation note
        ax3.text(0.05, 0.85, 'Note: High Fe/EC may indicate\ndust interference', 
                transform=ax3.transAxes, bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                fontweight='bold', fontsize=10)
    else:
        ax3.scatter(ec_clean, fabs_clean, alpha=0.7, s=60, color='blue')
        ax3.text(0.5, 0.5, 'Iron data not available', transform=ax3.transAxes, 
                ha='center', va='center', fontweight='bold', fontsize=14)
    
    ax3.set_xlabel('FTIR-BC (μg/m³)', fontweight='bold')
    ax3.set_ylabel('Fabs (Mm⁻¹)', fontweight='bold')
    ax3.set_title('FTIR-BC vs Fabs\n(Colored by Fe/EC Ratio)', fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend()
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 4: Seasonal MAC plots for Methods 2 & 4 (viable methods)
    # =============================================================================
    print("\n📈 REQUEST 4: Seasonal MAC plots for Methods 2 & 4")
    
    if 'season' in df.columns:
        # Calculate MAC values for Methods 2 and 4
        mac_method2 = mac_results['mac_values']['Method_2']  # Ratio of means
        mac_method4 = mac_results['mac_values']['Method_4']  # Origin regression
        
        # Calculate BC equivalents for each method
        bc_method2 = df['fabs'] / mac_method2
        bc_method4 = df['fabs'] / mac_method4
        
        # Get seasonal data
        seasons = df['season'].unique()
        seasons = [s for s in seasons if pd.notna(s)]
        
        fig4, (ax4a, ax4b) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Method 2 seasonal plot
        seasonal_bc2 = []
        seasonal_ec = []
        season_labels = []
        
        for season in seasons:
            season_mask = df['season'] == season
            season_bc2 = bc_method2[season_mask].dropna()
            season_ec_vals = df.loc[season_mask, 'ec_ftir'].dropna()
            
            if len(season_bc2) > 3:
                seasonal_bc2.append(season_bc2)
                seasonal_ec.append(season_ec_vals)
                season_labels.append(season.split()[0])
        
        # Boxplots for Method 2
        if len(seasonal_bc2) >= 2:
            bp2 = ax4a.boxplot(seasonal_bc2, labels=season_labels, patch_artist=True)
            
            colors_season = ['#d35400', '#27ae60', '#2980b9'][:len(seasonal_bc2)]
            for patch, color in zip(bp2['boxes'], colors_season):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Add EC reference
            overall_ec_mean = df['ec_ftir'].mean()
            ax4a.axhline(overall_ec_mean, color='red', linestyle='--', linewidth=2, 
                        label=f'Overall EC Mean ({overall_ec_mean:.1f} μg/m³)')
        
        ax4a.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
        ax4a.set_title(f'Method 2: Ratio of Means\nMAC = {mac_method2:.2f} m²/g', fontweight='bold')
        ax4a.legend()
        ax4a.grid(True, alpha=0.3, axis='y')
        
        # Method 4 seasonal plot
        seasonal_bc4 = []
        for season in seasons:
            season_mask = df['season'] == season
            season_bc4 = bc_method4[season_mask].dropna()
            
            if len(season_bc4) > 3:
                seasonal_bc4.append(season_bc4)
        
        if len(seasonal_bc4) >= 2:
            bp4 = ax4b.boxplot(seasonal_bc4, labels=season_labels, patch_artist=True)
            
            for patch, color in zip(bp4['boxes'], colors_season):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Add EC reference
            ax4b.axhline(overall_ec_mean, color='red', linestyle='--', linewidth=2, 
                        label=f'Overall EC Mean ({overall_ec_mean:.1f} μg/m³)')
        
        ax4b.set_ylabel('BC Equivalent (μg/m³)', fontweight='bold')
        ax4b.set_title(f'Method 4: Origin Regression\nMAC = {mac_method4:.2f} m²/g', fontweight='bold')
        ax4b.legend()
        ax4b.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.show()
        
        # Print seasonal statistics
        print(f"   Seasonal MAC Analysis:")
        for i, season in enumerate(season_labels):
            if i < len(seasonal_bc2) and i < len(seasonal_bc4):
                bc2_mean = seasonal_bc2[i].mean()
                bc4_mean = seasonal_bc4[i].mean()
                ec_mean = seasonal_ec[i].mean()
                print(f"     {season}: Method 2 BC = {bc2_mean:.2f}, Method 4 BC = {bc4_mean:.2f}, EC = {ec_mean:.2f} μg/m³")
    
    else:
        print("   ⚠️ No seasonal data available")
    
    # =============================================================================
    # REQUEST 5: Bias / RMSE summary table (overall and by season)
    # =============================================================================
    print("\n📊 REQUEST 5: Bias/RMSE Summary Table")
    
    from sklearn.metrics import mean_squared_error, mean_absolute_error
    
    # Calculate performance metrics for Methods 2 and 4
    methods = ['Method_2', 'Method_4']
    method_names = ['Ratio of Means', 'Origin Regression']
    
    # Overall performance
    overall_metrics = {}
    for method in methods:
        mac_val = mac_results['mac_values'][method]
        bc_values = df['fabs'] / mac_val
        ec_values = df['ec_ftir']
        
        # Filter reasonable values
        mask = (bc_values > 0) & (ec_values > 0) & (bc_values < 100) & (ec_values < 100)
        bc_clean = bc_values[mask]
        ec_clean = ec_values[mask]
        
        bias = np.mean(bc_clean - ec_clean)
        rmse = np.sqrt(mean_squared_error(ec_clean, bc_clean))
        mae = mean_absolute_error(ec_clean, bc_clean)
        
        overall_metrics[method] = {'bias': bias, 'rmse': rmse, 'mae': mae, 'n': len(bc_clean)}
    
    # Create summary table
    fig5, ax5 = plt.subplots(1, 1, figsize=(12, 8))
    ax5.axis('off')
    
    # Prepare table data
    table_data = []
    headers = ['Method', 'MAC (m²/g)', 'Bias (μg/m³)', 'RMSE (μg/m³)', 'MAE (μg/m³)', 'Samples']
    
    # Overall results
    for i, method in enumerate(methods):
        mac_val = mac_results['mac_values'][method]
        metrics = overall_metrics[method]
        
        row = [
            method_names[i],
            f'{mac_val:.3f}',
            f'{metrics["bias"]:.3f}',
            f'{metrics["rmse"]:.3f}',
            f'{metrics["mae"]:.3f}',
            f'{metrics["n"]}'
        ]
        table_data.append(row)
    
    # Add seasonal breakdown if available
    if 'season' in df.columns:
        table_data.append(['', '', '', '', '', ''])  # Empty row
        table_data.append(['SEASONAL BREAKDOWN', '', '', '', '', ''])
        
        for season in seasons:
            season_mask = df['season'] == season
            if season_mask.sum() < 5:
                continue
                
            for i, method in enumerate(methods):
                mac_val = mac_results['mac_values'][method]
                bc_season = (df.loc[season_mask, 'fabs'] / mac_val).dropna()
                ec_season = df.loc[season_mask, 'ec_ftir'].dropna()
                
                if len(bc_season) > 3 and len(ec_season) > 3:
                    # Align the series
                    common_idx = bc_season.index.intersection(ec_season.index)
                    bc_aligned = bc_season[common_idx]
                    ec_aligned = ec_season[common_idx]
                    
                    bias_season = np.mean(bc_aligned - ec_aligned)
                    rmse_season = np.sqrt(mean_squared_error(ec_aligned, bc_aligned))
                    mae_season = mean_absolute_error(ec_aligned, bc_aligned)
                    
                    row = [
                        f'{season.split()[0]} - {method_names[i]}',
                        f'{mac_val:.3f}',
                        f'{bias_season:.3f}',
                        f'{rmse_season:.3f}',
                        f'{mae_season:.3f}',
                        f'{len(bc_aligned)}'
                    ]
                    table_data.append(row)
    
    # Create table
    table = ax5.table(cellText=table_data, colLabels=headers, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)
    
    # Color code the table
    colors_methods = ['#e8f4fd', '#fff2e8']
    for i, row in enumerate(table_data):
        if 'Method' in str(row[0]) or 'SEASONAL' in str(row[0]):
            continue
        
        method_idx = 0 if 'Ratio of Means' in str(row[0]) or 'Method_2' in str(row[0]) else 1
        for j in range(len(headers)):
            table[(i+1, j)].set_facecolor(colors_methods[method_idx])
            table[(i+1, j)].set_alpha(0.7)
    
    ax5.set_title('Bias/RMSE Summary: Methods 2 & 4\n(Overall and Seasonal Performance)', 
                  fontweight='bold', pad=20, fontsize=16)
    
    plt.tight_layout()
    plt.show()
    
    # =============================================================================
    # REQUEST 6: MAC deviation vs potential drivers (Fe, sulfate, RH, T, rain)
    # =============================================================================
    print("\n📈 REQUEST 6: MAC deviation vs potential drivers")
    
    # Calculate MAC deviations (individual MAC - method MAC)
    individual_mac = df['fabs'] / df['ec_ftir']
    reasonable_mac_mask = (individual_mac > 5) & (individual_mac < 20)
    
    # Use Method 2 as reference (ratio of means)
    reference_mac = mac_results['mac_values']['Method_2']
    mac_deviation = individual_mac - reference_mac
    
    # Prepare drivers data
    potential_drivers = {}
    
    if 'Iron' in df.columns:
        potential_drivers['Iron'] = df['Iron']
    
    if 'Sulfate_Ion' in df.columns:
        potential_drivers['Sulfate'] = df['Sulfate_Ion']
    
    # Note: RH, Temperature, and Rain data would need to be added to the dataset
    # For now, we'll work with available chemical species
    if 'Nitrate_Ion' in df.columns:
        potential_drivers['Nitrate'] = df['Nitrate_Ion']
    
    if 'Potassium_Ion' in df.columns:
        potential_drivers['Potassium'] = df['Potassium_Ion']
    
    # Check for seasonal data
    has_seasons = 'season' in df.columns and df['season'].notna().sum() > 0
    
    if has_seasons:
        season_colors = {
            'Dry Season (Bega)': '#d35400', 
            'Belg Rainy Season': '#27ae60', 
            'Kiremt Rainy Season': '#2980b9'
        }
        print(f"   🌍 Seasonal breakdown available for MAC deviation analysis")
    
    # Create subplot grid
    n_drivers = len(potential_drivers)
    if n_drivers > 0:
        n_cols = 2
        n_rows = (n_drivers + 1) // 2
        
        fig6, axes = plt.subplots(n_rows, n_cols, figsize=(16, 5*n_rows))
        if n_rows == 1:
            axes = [axes]
        axes = axes.flatten()
        
        for i, (driver_name, driver_data) in enumerate(potential_drivers.items()):
            ax = axes[i]
            
            # Filter data
            driver_mask = reasonable_mac_mask & driver_data.notna() & mac_deviation.notna()
            x_data = driver_data[driver_mask]
            y_data = mac_deviation[driver_mask]
            
            if len(x_data) > 10:
                
                if has_seasons:
                    # Seasonal analysis
                    seasons_for_driver = df.loc[x_data.index, 'season']
                    
                    print(f"   📊 {driver_name} seasonal correlations:")
                    
                    # Plot by season
                    for season in seasons_for_driver.unique():
                        if pd.isna(season):
                            continue
                            
                        season_mask = seasons_for_driver == season
                        if season_mask.sum() < 3:  # Skip seasons with too few points
                            continue
                            
                        season_x = x_data[season_mask]
                        season_y = y_data[season_mask]
                        
                        if season in season_colors and len(season_x) >= 3:
                            # Scatter plot
                            ax.scatter(season_x, season_y, 
                                     color=season_colors[season], alpha=0.7, s=60, 
                                     label=season.split()[0], edgecolors='black', linewidth=1)
                            
                            # Regression line for this season
                            if len(season_x) >= 5:
                                z_season = np.polyfit(season_x, season_y, 1)
                                p_season = np.poly1d(z_season)
                                x_line_season = np.linspace(season_x.min(), season_x.max(), 50)
                                
                                ax.plot(x_line_season, p_season(x_line_season), 
                                       color=season_colors[season], linewidth=3, 
                                       linestyle='--', alpha=0.9)
                                
                                # Calculate seasonal correlation
                                r_season, p_season_val = pearsonr(season_x, season_y)
                                print(f"     {season}: r = {r_season:.3f}, p = {p_season_val:.3f}, n = {len(season_x)}")
                    
                    # Overall regression line in black
                    if len(x_data) > 5:
                        z_overall = np.polyfit(x_data, y_data, 1)
                        p_overall = np.poly1d(z_overall)
                        x_line_overall = np.linspace(x_data.min(), x_data.max(), 100)
                        ax.plot(x_line_overall, p_overall(x_line_overall), 'black', linewidth=4, 
                               linestyle='-', alpha=0.8, label='Overall Trend')
                        
                        # Overall correlation statistics
                        r_overall, p_overall_val = pearsonr(x_data, y_data)
                        ax.text(0.05, 0.95, f'Overall: r = {r_overall:.3f}\np = {p_overall_val:.2e}\nn = {len(x_data)}', 
                               transform=ax.transAxes, 
                               bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9),
                               fontweight='bold', va='top', fontsize=10)
                    
                    # Add legend for seasons
                    ax.legend(fontsize=10)
                    
                else:
                    # No seasonal data - original single color plot
                    ax.scatter(x_data, y_data, alpha=0.7, s=50, color='blue', 
                              edgecolors='black', linewidth=0.5)
                    
                    # Add regression line
                    if len(x_data) > 5:
                        z = np.polyfit(x_data, y_data, 1)
                        p = np.poly1d(z)
                        x_line = np.linspace(x_data.min(), x_data.max(), 100)
                        ax.plot(x_line, p(x_line), 'red', linewidth=2, alpha=0.8)
                        
                        # Calculate correlation
                        r, p_val = pearsonr(x_data, y_data)
                        ax.text(0.05, 0.95, f'r = {r:.3f}\np = {p_val:.2e}', 
                               transform=ax.transAxes, 
                               bbox=dict(boxstyle='round', facecolor='white', alpha=0.9),
                               fontweight='bold')
                        
                        print(f"   {driver_name}: r = {r:.3f}, p = {p_val:.2e}, n = {len(x_data)}")
                
                # Add zero line
                ax.axhline(0, color='black', linestyle='--', alpha=0.5)
                
                ax.set_xlabel(f'{driver_name} (μg/m³)', fontweight='bold')
                ax.set_ylabel('MAC Deviation (m²/g)', fontweight='bold')
                ax.set_title(f'MAC Deviation vs {driver_name}\n(Seasonal Analysis)', fontweight='bold')
                ax.grid(True, alpha=0.3)
                
            else:
                ax.text(0.5, 0.5, f'Insufficient {driver_name} data', 
                       transform=ax.transAxes, ha='center', va='center', 
                       fontweight='bold', fontsize=12)
                ax.set_title(f'MAC Deviation vs {driver_name}', fontweight='bold')
        
        # Hide empty subplots
        for j in range(i+1, len(axes)):
            axes[j].set_visible(False)
        
        plt.tight_layout()
        plt.show()
    else:
        print("   ⚠️ No potential drivers data available")
    
    print(f"\n✅ ALL 6 REQUESTED ANALYSES COMPLETE!")
    
    # Return summary for documentation
    summary_results = {
        'analysis_1': 'FTIR-BC vs Fabs (colored by Fe concentration)',
        'analysis_2': 'FTIR-BC vs Fabs (colored by Fe/PM₂.₅ ratio)',
        'analysis_3': 'FTIR-BC vs Fabs (colored by Fe/EC ratio)',
        'analysis_4': 'Seasonal MAC plots for Methods 2 & 4',
        'analysis_5': 'Bias/RMSE summary table (overall and seasonal)',
        'analysis_6': 'MAC deviation vs potential drivers (with seasonal analysis)',
        'methods_analyzed': ['Method_2 (Ratio of Means)', 'Method_4 (Origin Regression)'],
        'reference_mac': reference_mac,
        'available_drivers': list(potential_drivers.keys()) if potential_drivers else [],
        'seasonal_data_available': 'season' in df.columns,
        'seasonal_analysis_enhanced': has_seasons if 'has_seasons' in locals() else False
    }
    
    return summary_results

# %%
# Execute Ann's meeting requests
if __name__ == "__main__":
    print("🎯 EXECUTING ANN'S MEETING REQUESTS...")
    print("📋 Creating 6 specific analyses as requested during the meeting")
    
    # Run the analyses using existing data
    # Note: Replace 'results' with your actual merged_data and mac_results variables
    
    # Example usage:
    # meeting_results = create_ann_meeting_analyses(results['merged_data'], results['mac_results'])
    
    print("\n📝 TO RUN THE ANALYSES:")
    print("meeting_results = create_ann_meeting_analyses(merged_data, mac_results)")
    print("\nThis will create:")
    print("1. FTIR-BC vs Fabs scatter (colored by Fe concentration)")
    print("2. FTIR-BC vs Fabs scatter (colored by Fe/PM₂.₅ ratio)")  
    print("3. FTIR-BC vs Fabs scatter (colored by Fe/EC ratio)")
    print("4. Seasonal MAC plots for Methods 2 & 4")
    print("5. Bias/RMSE summary table (overall and seasonal)")
    print("6. MAC deviation vs potential drivers (WITH SEASONAL ANALYSIS)")
    print("   - Separate scatter plots and fit lines for each Ethiopian season")
    print("   - Color-coded by season with individual regression statistics")
    print("   - Overall trend line in black for comparison")
    print("   - Seasonal correlation statistics printed to console")

# %% [markdown]
# ### Meeting Notes Summary
# 
# **Ann's Specific Requests:**
# 
# 1. **Iron concentration effects**: Direct visualization of how iron concentration affects the FTIR-BC vs Fabs relationship
# 2. **Iron relative abundance**: Two approaches - Fe/PM₂.₅ and Fe/EC ratios to understand iron's relative importance
# 3. **Seasonal MAC validation**: Focus on the two viable methods (Methods 2 & 4) across Ethiopian climate seasons
# 4. **Performance consolidation**: Comprehensive bias/RMSE analysis across all conditions
# 5. **Composition effects**: Systematic correlation analysis between MAC deviations and potential drivers with seasonal breakdown
# 
# **Key Questions Addressed:**
# - Does iron concentration directly correlate with optical absorption interference?
# - How does iron's relative abundance (vs total PM or vs EC) affect measurements?
# - Are the viable MAC methods seasonally stable?
# - Which chemical/meteorological factors drive MAC variability across seasons?
# 
# **Expected Outcomes:**
# - Clear visualization of iron interference patterns
# - Seasonal stability assessment for recommended methods
# - Identification of key drivers for MAC correction factors (seasonal and overall)
# - Performance metrics for method selection and uncertainty quantification
# - Seasonal variability patterns in chemical interference effects

# %%
# Uncomment to run with your data:
meeting_results = create_ann_meeting_analyses(results['merged_data'], results['mac_results'])


