import pandas as pd

t_start = '2021-8-1 18:00'
t_end = '2021-8-1 18:01'
times = pd.date_range(start=t_start, end=t_end, freq='1h')
for pos, time in enumerate(times):
    if (18 <= time.hour <= 19) and time.replace(minute=30) < times[-1]:
        times = times.insert(pos+1, time.replace(minute=30))

    c = 4
times1 = times.sort_values()
a = 5
