from src.data_base.query_data import QueryFromDB

class streamingNode():
    # TODO some functions should be added
    # TODO write code to functions
    
    def __init__(self):
        # TODO create config

        self.token = ""
        self.url = "http://localhost:8086"
        self.org = "TestOrg"
        self.bucket = "TestBucket"
        pass

    def connectToDB(self):
        # TODO Change QueryFromDB __init__ to accept argument
        # I will do it after some testing.
        QueryFromDB(self.token, self.url, self.org, self.bucket)

    def checkDataAvailability(self):
        data_available = True
        # TODO
        
        return data_available

    def createAggregates(self):
        pass

    def getAgregates(self):
        pass

    def getPartialFeatureVector(self):
        vector = []
        timestamp = [] # time for fussion
        


        return vector