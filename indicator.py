
class Line:
    """具体指标的抽象类"""
    def __init__(self,group, limit):
        self.group = group.tail(limit)
        self.length = len(self.group)
    
    def __getitem__(self, col):
        return self.group[col].values


class Indicator:
    """指标管理类"""
    def __init__(self, data, limit=60, groupk='instId'):
        self.groups = data.groupby(groupk)
        self._dict_ = dict()
        self.init(limit)
        
    def init(self, limit):
        for (instId, group) in self.groups:
            self._dict_[instId] = Line(group, limit)
        
    def __getitem__(self, instId):
        return self._dict_.get(instId)
        
        
        