# Visualization

MONET provides a variety of plotting functions for atmospheric and air quality data, ranging from quick map plots to specialized diagrams like Taylor diagrams.

## Map Plots

While the `.monet` accessor provides quick map methods (`quick_map`, `quick_imshow`, `quick_contourf`), MONET also includes standalone plotting functions in `monet.plots`.

### Wind Plots
Visualize wind fields using quivers or barbs.

```python
from monet.plots.plots import wind_quiver, wind_barbs

# Create a wind quiver plot
fig, ax = wind_quiver(u_da, v_da, thin=10)

# Create a wind barbs plot
fig, ax = wind_barbs(u_da, v_da, thin=15)
```

### Bias Scatter Plots
Visualize the spatial distribution of model bias.

```python
from monet.plots.plots import spatial_bias_scatter

# ds must contain 'obs' and 'model' variables
fig, ax = spatial_bias_scatter(ds, cmap='RdBu_r')
```

## Statistical Plots

### Time Series with Error Shading
Plot time series from a DataFrame with automatic grouping and ±1 SD shading.

```python
from monet.plots.plots import timeseries

# df should have 'time', 'obs', 'variable', and 'units'
ax = timeseries(df, y='obs', title='Ozone Time Series')
```

### Kernel Density Estimate (KDE)
```python
from monet.plots.plots import kdeplot

ax = kdeplot(df['obs'], title='Distribution of Observations')
```

### Taylor Diagrams
Taylor diagrams are used to concisely summarize multiple aspects of model performance (correlation, RMS error, and standard deviation) in a single plot.

```python
from monet.plots.plots import create_taylor_diagram

# Create a Taylor diagram for a model
dia = create_taylor_diagram(obs_series, model_series, model_label='Model 1')

# Add a second model to the same diagram
create_taylor_diagram(obs_series, model2_series, model_label='Model 2', dia=dia)
```

## Plot Customization

Many MONET plotting functions return Matplotlib `Figure` and `Axes` objects, allowing for further customization using standard Matplotlib and Cartopy commands.

```python
fig, ax = ds.O3.monet.quick_map()
ax.set_extent([-130, -60, 20, 50]) # Zoom to CONUS
fig.set_size_inches(12, 6)
```
