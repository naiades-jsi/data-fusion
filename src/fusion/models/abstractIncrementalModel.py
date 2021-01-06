class AbstractIncrementalModel():
    def __init__(self, options, fusion):
        self.options = options

    def partialFit(self, featureVec, label):
        pass
    
    def predict(self, featureVec):
        pass
    
    def save(self):
        pass
    
    def load(self):
        pass