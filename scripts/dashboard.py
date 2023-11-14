import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Function to load and prepare dataset


def load_data(file_path):
    data = pd.read_csv(file_path)
    # get date from columns YEAR, MONTH, DAY
    data['DATE'] = pd.to_datetime(data[['YEAR', 'MONTH', 'DAY']].fillna(1).astype(
        int).apply(lambda row: '-'.join(row.values.astype(str)), axis=1))
    return data


# Load datasets
data_arid = load_data('data/clustered_data_ARID.csv')
data_humid = load_data('data/clustered_data_HUMID.csv')
data_hyper_humid = load_data('data/clustered_data_HYPER ARID.csv')
data_semi_arid = load_data('data/clustered_data_SEMI ARID.csv')

st.title('Climate Analysis Dashboard')
st.markdown("""
    This dashboard provides an in-depth analysis of temperature records for different regions. 
    Utilize the sidebar to filter results based on region, year range, and other parameters.
    """)

# Sidebar for filters
st.sidebar.title('Filters')

# Region Selector
region = st.sidebar.radio(
    'Select Region', ['ARID', 'HUMID', 'HYPER ARID', 'SEMI ARID'])
data = data_arid if region == 'ARID' else data_humid if region == 'HUMID' else data_hyper_humid if region == 'HYPER ARID' else data_semi_arid

# Year Range Selector in Sidebar
year_min, year_max = int(data['YEAR'].min()), int(data['YEAR'].max())
selected_years = st.sidebar.slider(
    'Select Year Range', year_min, year_max, (year_min, year_max))

# Temperature Smoothing Slider
smoothing_window = st.sidebar.slider('Smoothing Window (Days)', 1, 365, 30)

# Aggregation Selector
aggregate_by = st.sidebar.selectbox(
    'Aggregate Data By', ['None', 'Month', 'Year'])

# Filter data by selected year range
filtered_data = data[(data['YEAR'] >= selected_years[0])
                     & (data['YEAR'] <= selected_years[1])]

# Store cluster data before aggregation
cluster_data = filtered_data['CLUSTER'].value_counts()

# Apply aggregation if selected
if aggregate_by == 'Month':
    filtered_data = filtered_data.groupby(
        ['YEAR', 'MONTH']).mean().reset_index()
elif aggregate_by == 'Year':
    filtered_data = filtered_data.groupby(['YEAR']).mean().reset_index()

# Apply smoothing if the window is greater than 1
if smoothing_window > 1:
    for col in ['AVERAGE', 'MAX', 'MIN']:
        filtered_data[col] = filtered_data[col].rolling(
            smoothing_window, min_periods=1).mean()

# If data is aggregated by month or year, adjust the index for plotting
if aggregate_by != 'None':
    filtered_data['DATE'] = pd.to_datetime(filtered_data[['YEAR', 'MONTH']].fillna(
        1).astype(int).apply(lambda row: '-'.join(row.values.astype(str)), axis=1))
else:
    filtered_data['DATE'] = pd.to_datetime(filtered_data['DATE'])

# Forecasting Section
st.sidebar.subheader('Forecasting')
enable_forecasting = st.sidebar.checkbox('Enable Forecasting')
forecast_period = 0
if enable_forecasting:
    forecast_period = st.sidebar.number_input(
        'Forecast Period (Months)', min_value=1, max_value=36, value=12)

st.header(f'Regional Climate Data: {region}')
st.markdown(f"""
    ### Temperature Over Time
    Observe how temperature trends change over time. Smoothing is applied to make trends more noticeable.
    """)


# Time Series Plot
st.subheader(f'Temperature Time Series for {region} Region')
temp_type = st.selectbox('Select Temperature Type', ['AVERAGE', 'MAX', 'MIN'])
st.line_chart(filtered_data[['DATE', temp_type]].set_index('DATE'))

st.markdown(f"""
    ### Temperature Distribution Analysis
    Explore the distribution of temperature data through histograms and box plots. 
    These visualizations help identify outliers and understand the spread of the data.
    """)

# Create the boxplot
# You can adjust the figure size if needed
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=filtered_data, x='YEAR', y='AVERAGE', ax=ax)

# Rotate the labels and only show every 5th label to reduce crowding
for index, label in enumerate(ax.xaxis.get_ticklabels()):
    if index % 5 != 0:  # Adjust the modulus to change frequency of labels
        label.set_visible(False)
    label.set_rotation(90)  # Rotate labels to vertical

ax.set_title(f'Box Plot of Average Temperature by YEAR for {region} Region')
st.pyplot(fig)


# Histograms
st.subheader(f'Temperature Distributions for {region} Region')
fig, ax = plt.subplots()
sns.histplot(filtered_data, x='AVERAGE', bins=30, kde=True, ax=ax)
ax.set_title(f'Average Temperature Distribution for {region} Region')
st.pyplot(fig)

st.markdown(f"""
    ### Monthly and Yearly Heatmaps
    Heatmaps provide a color-coded view of temperature changes across months and years, 
    allowing for quick identification of patterns and anomalies.
    """)

# Heatmap Visualization
st.subheader(f'Heatmap of {temp_type} Temperature for {region} Region')

# Aggregate data by MONTH regardless of the user's selection for the heatmap
heatmap_data = filtered_data.groupby(['MONTH', 'YEAR'])[
    temp_type].mean().unstack()
fig, ax = plt.subplots(figsize=(9, 5))
sns.heatmap(heatmap_data, cmap='coolwarm', ax=ax)
ax.set_title(f'Heatmap of {temp_type} Temperature for {region} Region')
st.pyplot(fig)

st.markdown(f"""
    ### Cluster Analysis
    The pie chart below shows the distribution of data across different clusters, 
    which can represent categorizations or types within the dataset.
    """)

# Cluster Analysis
st.subheader(f'Cluster Analysis for {region} Region')
fig, ax = plt.subplots(figsize=(3, 3))
ax.pie(cluster_data, labels=cluster_data.index, autopct='%1.1f%%')
ax.set_title(f'Cluster Distribution for {region} Region')
st.pyplot(fig)

if enable_forecasting:
    st.markdown("""
        ### Forecasting Future Temperatures
        The following section provides a forecast of future temperatures based on historical data.
        Adjust the forecast horizon in the sidebar to see predictions for different timeframes.
        """)
    if aggregate_by != 'Month':
        st.error('Forecasting requires data to be aggregated by Month.')
    else:
        # Proceed with forecasting
        model_data = filtered_data.set_index(
            'DATE').resample('M').mean().dropna()

        # Fit the SARIMAX model
        sarimax_model = SARIMAX(model_data['AVERAGE'],
                                order=(1, 1, 1),
                                seasonal_order=(1, 1, 1, 12),
                                enforce_stationarity=False,
                                enforce_invertibility=False)

        sarimax_result = sarimax_model.fit(disp=False)

        # Get forecast 'forecast_period' steps ahead in future
        forecast = sarimax_result.get_forecast(steps=forecast_period)
        forecast_index = pd.date_range(
            model_data.index[-1], periods=forecast_period, freq='M')
        forecast_data = pd.Series(
            forecast.predicted_mean, index=forecast_index)

        # Plotting the forecast
        st.subheader('Temperature Forecast')
        fig, ax = plt.subplots(figsize=(10, 6))
        model_data['AVERAGE'].plot(ax=ax, label='Observed')
        forecast_data.plot(ax=ax, label='Forecast', alpha=0.7, linestyle='--')
        ax.fill_between(forecast_index,
                        forecast.conf_int().iloc[:, 0],
                        forecast.conf_int().iloc[:, 1], color='k', alpha=0.2)
        plt.legend()
        st.pyplot(fig)

st.markdown("---")
st.markdown("""
    *Data and forecasts are provided for informational purposes only and are not a substitute for professional advice.*
    """)
