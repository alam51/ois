import pandas as pd
import numpy as np
from df_to_html_with_style import Df2HtmlWithStyle

input_str_in_df = '_input51'
input_str_in_html = '<input type="number" name="name" size=30 >'
c1 = pd.MultiIndex.from_product(['Bus'])
columns = pd.MultiIndex.from_product([['Decision Tree', 'Regression', 'Random'],
                                      ['Tumour', 'Non-Tumour', 'test1', 'test2']],
                                     names=['Model:', 'Predicted:'])
df = pd.DataFrame([[38.0, 2.0, 18.0, 22.0, 21, np.nan, 1, 2, 3, 4, 5, 6],
                   [19, input_str_in_html, 6, 452, 226, 232, 1, 2, 3, 4, 5, 6]],
                  index=pd.Index(['Tumour (Positive)', 'Non-Tumour (Negative)'],
                                 # name='Actual Label:'
                                 ),
                  columns=columns)

html_relative_path = "hello1.html"

Df2HtmlWithStyle(raw_df=df, output_html_path=html_relative_path)
