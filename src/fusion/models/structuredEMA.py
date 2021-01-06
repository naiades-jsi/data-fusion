from abstractIncrementalModel import AbstractIncrementalModel

class StructuredEMAIncrementalModel(AbstractIncrementalModel):
    def __init__(self, options, fusion):
        super().__init__(options, fusion)
        
        self.value = 0
        optionN = options['N'] if options['N'] != None else 5
        self.k = 2 / (optionN + 1)
        self.EMA = {}
        
    def partialFit(self, featureVec, label):
        structuralFactor = featureVec[self.options['structuralFactorPosition']]

        #if self.EMA[structuralFactor] == None:
        #    self.EMA[structuralFactor] = label
        #else:
        #    self.EMA[structuralFactor] = label * self.k + (1 - self.k) * self.EMA[structuralFactor]

        #print(structuralFactor)
        
        try:
            self.EMA[structuralFactor] = label * self.k + (1 - self.k) * self.EMA[structuralFactor]
        except:
            self.EMA[structuralFactor] = label

    def predict(self, featureVec):
        structuralFactor = featureVec[self.options['structuralFactorPosition']]

        try:
            return self.EMA[structuralFactor]
        except:
            return None
    
    def save(self, fout):
        pass
    
    def load(self, fin):
        pass