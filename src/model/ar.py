import datetime as dt
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

def autoplot(data, biweeks=39, title='Forecast of Demand for H-1B Visas'):
    """
    pass
    """
    # Sum Total Workers for Each Date
    data = data.groupby('CASE_SUBMITTED').sum()
    
    # Convert Index to DatetimeIndex
    data.index = pd.to_datetime(data.index)
    
    # Group Data by Semi-Month Start Frequency (SMS)
    data = data.resample('SMS').sum()
    
    # ¯\_(ツ)_/¯ Code Needs This to Work
    data.index = [dt.datetime.strftime(date, '%Y-%m-%d') for date in data.index]
    data.index = [dt.datetime.strptime(date, '%Y-%m-%d').date() for date in data.index]
    
    # Model
    START = data.shape[0] - 2
    
    model = AR(data['TOTAL_WORKERS']).fit(maxlag=25)
    y_hat = model.predict(START, START + biweeks, dynamic=True)
    y_hat.loc[START] = data.iloc[-1][0]
    
    START = data.index[-1]
    
    y_hat.index = pd.date_range(START, START + dt.timedelta(weeks=biweeks**2), freq='SMS')[:biweeks + 1]
    y_hat.index = [dt.datetime.strftime(date, '%Y-%m-%d') for date in y_hat.index]
    y_hat.index = [dt.datetime.strptime(date, '%Y-%m-%d').date() for date in y_hat.index]
    
    # Visualization
    ax = plt.gca()
    
    ax.plot(
        data['TOTAL_WORKERS'],
        label='Observed',
        alpha=0.75,
        marker='.',
        mfc='red'
    )
    ax.plot(
        y_hat,
        label='Forecast',
        alpha=0.75,
        marker='.',
        mfc='red'
    )
    
    ax.set_xlim(dt.date(data.index[0].year, 1, 1), dt.date(y_hat.index[-1].year + 1, 1, 1))
    ax.xaxis.set_major_formatter(DateFormatter('%b %Y'))
    ax.set_ylim(-0.1e5, 2.1e5)
    ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
    
    ax.set_title(title)
    ax.legend(loc='best');