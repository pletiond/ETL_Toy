import numpy


class Data:
    """
    Class for data storage
    Using 2D numpy array for data and list for column names
    """
    columns_names = []
    data = None

    def add_row(self, values):
        """
        Add new row  to dataset

        :param values: list of new values

        """
        if self.data is None:
            self.data = numpy.array(values)
        else:
            self.data = numpy.vstack((self.data, values))


    def add_column_name(self, name):
        self.columns_names.append(name)

    def remove_column(self, column_name):
        """
        Remove selected column

        :param column_name:
        :return: False if column doesnt exist

        """
        if not column_name in self.columns_names:
            return False
        number = self.columns_names.index(column_name)
        self.columns_names.pop(number)
        self.data = numpy.delete(self.data, number, 1)
        return True

    def sort_by_column(self, column):
        """
        Sort data by selected column

        :param column: sorting column
        :return: False if sorting column doesnt exist

        """
        if not column in self.columns_names:
            return False
        index = self.columns_names.index(column)
        self.data = self.data[self.data[:, index].argsort()]
        return True
