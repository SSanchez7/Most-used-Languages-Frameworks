import pandas as pd
import numpy as np
import os
import bar_chart_race as bcr
import matplotlib as mt

if __name__ == "__main__":
	questTagByDate = pd.read_csv(os.path.join('questTagByDateCsv/part-00000'), names=['date','tag','count'])
	questTagByDate = questTagByDate.pivot_table(index='date', columns='tag', values=['count'], fill_value=0)
	bcr_html = bcr.bar_chart_race(df=questTagByDate, filename=None)