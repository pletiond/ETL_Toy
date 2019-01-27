import numpy


class Data:
    columns_names = []
    columns_dtypes = []
    data = None

    def add_row(self, values):
        # print(values)
        if self.data is None:
            self.data = numpy.array(values)
        else:
            self.data = numpy.vstack((self.data, values))
        # self.data.append(values)

    def add_column_name(self, name, dtype=''):
        self.columns_names.append(name)
        self.columns_dtypes.append(dtype)

    def remove_column(self, column_name):
        number = self.columns_names.index(column_name)
        self.columns_names.pop(number)
        self.columns_dtypes.pop(number)
        self.data = numpy.delete(self.data, number, 1)
