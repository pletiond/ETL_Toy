import numpy


class Data:
    columns_names = []
    data = None

    def add_row(self, values):
        if self.data is None:
            self.data = numpy.array(values)
        else:
            self.data = numpy.vstack((self.data, values))
        # self.data.append(values)

    def add_column_name(self, name):
        self.columns_names.append(name)

    def remove_column(self, column_name):
        number = self.columns_names.index(column_name)
        self.columns_names.pop(number)
        self.data = numpy.delete(self.data, number, 1)

    def sort_by_column(self, column):
        index = self.columns_names.index(column)
        self.data = self.data[self.data[:, index].argsort()]
