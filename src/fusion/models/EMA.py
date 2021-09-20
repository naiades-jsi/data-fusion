from abstractIncrementalModel import AbstractIncrementalModel

# TODO EMA is also in influxDB add it and test.

class EMAIncrementalModel(AbstractIncrementalModel):
    def __init__(self, options, fusion):
        super().__init__(options, fusion)
        
        self.value = 0
        optionN = options['N'] if options['N'] != None else 5
        self.k = 2 / (optionN + 1)
        self.EMA = None
        
    def partialFit(self, featureVec, label):
        if self.EMA == None:
            self.EMA = label
        else:
            self.EMA = label * self.k + (1 - self.k) * self.EMA
            
    def predict(self, featureVec):
        return self.EMA
    
    def save(self, fout):
        pass
    
    def load(self, fin):
        pass