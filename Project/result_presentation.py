from pyspark.sql.functions import col, countDistinct, count
import matplotlib.pyplot as plt
import pandas as pd

class ResultPresentation:
    df = None

    def __init__(self, df):
        self.df = df

    def get_value_count_for_column(self, column_name, skip_values=['Unknow', '-99']):
        disct_val_in_col = self.df.groupBy(str(column_name)).count()
        return disct_val_in_col
 
    def get_value_count_for_columns(self, columns):
        for col in columns:
            result_df = self.get_value_count_for_column(col)
            # self.save_df_to_file(col, result_df)
            self.print_and_save_plot(col, result_df, False)

    def get_filepath(self, column_name, extension):
        return 'column_results/' + column_name + '.' + extension

    def save_df_to_file(self, column_name, data):
        data.toPandas().to_csv(self.get_filepath(column_name, 'csv'))

    def print_and_save_plot(self, column_name, data, do_print, n_top=5):
        df = data.toPandas()
        df_top_data = df.nlargest(columns='count', n = n_top)
        plt.figure(figsize=(8,6))

        slices = df_top_data['count']
        categories = df_top_data[column_name]
        cols = ['purple', 'red', 'green', 'orange', 'dodgerblue']

        # Plotting the pie-chart.
        plt.pie(slices,
                labels = categories,
                colors=cols,
                startangle = 90,
                autopct = '%1.1f%%'
            )

        plt.title('5 najczęściej występujących ' + column_name, fontsize = 12)
        plt.savefig(self.get_filepath(column_name, 'jpeg'), dpi = 400)
        if do_print:
            plt.show()
        plt.clf()

