import datetime as dt
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from statsmodels.tsa.ar_model import AR

# % matplotlib inline
import matplotlib
matplotlib.rcParams['figure.figsize'] = 12, 4
matplotlib.rcParams['figure.dpi'] = 300

import matplotlib.pyplot as plt
plt.style.use('bmh')

from matplotlib.dates import DateFormatter
from matplotlib.ticker import FuncFormatter

def autoplot_val(data, title='Forecast of Demand for H-1B Visas', ylim=(-0.1e5, 1.1e5)):
    """
    pass
    """
    # Sum Total Workers for Each Date
    data = data.groupby('CASE_SUBMITTED').sum()
    
    # Convert Index to DatetimeIndex
    data.index = pd.to_datetime(data.index)
    
    # Group Data by Semi-Month Start Frequency (SMS)
    data = data.resample('SMS').sum()
    
    # Model
    END = data.shape[0] - 1
    START = END - len(data[data.index.year == data.index.year[-1]])
    
    model = AR(data['TOTAL_WORKERS']).fit(maxlag=25)
    y_hat = model.predict(START, END, dynamic=True)    
    y_hat.loc[data.index[START]] = data.iloc[START][0]
    
    y_true = data.iloc[START:].values    
    y_pred = np.array(y_hat).reshape(len(y_hat), 1)
    
    START = str(data.index.year[-1] - 1) + '-12-15'
    END = str(data.index.year[-1]) + '-12-15'
    
    y_hat.index = pd.date_range(START, END, freq='SMS')
    y_hat.index = [dt.datetime.strftime(date, '%Y-%m-%d') for date in y_hat.index]
    y_hat.index = [dt.datetime.strptime(date, '%Y-%m-%d').date() for date in y_hat.index]
    
    # ¯\_(ツ)_/¯ Code Needs This to Work
    data.index = [dt.datetime.strftime(date, '%Y-%m-%d') for date in data.index]
    data.index = [dt.datetime.strptime(date, '%Y-%m-%d').date() for date in data.index]
    
    # Visualization
    ax = plt.gca()
    
    ax.plot(
        data['TOTAL_WORKERS'],
        label='y_true',
        alpha=0.75,
        marker='.',
        mfc='red'
    )
    ax.plot(
        y_hat,
        label='y_pred',
        alpha=0.75,
        marker='.',
        mfc='red'
    )
    
    ax.set_xlim(dt.date(data.index[0].year, 1, 1), dt.date(y_hat.index[-1].year + 1, 1, 1))
    ax.xaxis.set_major_formatter(DateFormatter('%b %Y'))
    ax.set_ylim(ylim[0], ylim[1])
    ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
    
    ax.set_title(title)
    ax.legend(loc='best');
    
    # Metrics
    E = y_pred - y_true
    
    SE = np.square(E)
    MSE = np.mean(SE)
    RMSE = np.sqrt(MSE)
    
    AE = np.abs(E)
    APE = AE / y_true
    MAPE = np.mean(APE) * 100
    
    print('RMSE: {:>8.3f}'.format(RMSE))
    print('MAPE: {:>8.3f}'.format(MAPE))