class Actor:
    def __init__(self,rank, year, gdppc, neighbor):
        self._rank = rank
        self._year = year
        self._gdppc = gdppc 
        self._neighbor = neighbor

    @property
    def rank(self):
        return self._rank

    @rank.setter
    def rank(self,rank):
         self.rank = rank

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self,year):
         self.year = year

    @property
    def gdppc(self):
        return self._gdppc

    @gdppc.setter
    def gdppc(self,gdppc):
         self.gdppc = gdppc

    @property
    def neighbor(self):
        return self._neighbor

    @neighbor.setter
    def neighbor(self,neighbor):
         self.neighbor = neighbor


    def __repr__(self):
        return f"rank: {str(self.rank)} year: {str(self.year)} gdppc: {str(self.gdppc)} neighbor: {str(self.neighbor)}"
