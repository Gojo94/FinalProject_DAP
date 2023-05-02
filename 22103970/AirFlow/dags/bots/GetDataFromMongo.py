from pymongo import MongoClient
import pandas as pd

# define function to load data from MongoDB into pandas dataframe
def get_data():
    # connect to the MongoDB instance using client
    #client = MongoClient("mongodb+srv://dap:dap@cluster0.kjubgpu.mongodb.net/test")
    client = MongoClient("mongodb://dap:dap@localhost:27017")
    
    # access the database and collection
    db = client['TrafficDataset']
    collection = db['NonMotorist_crashes']

    # load data from collection into a dataframe
    loaded_data = collection.find()
    df = pd.DataFrame(loaded_data)

    # drop the default id column added by MongoDB
    df.drop(['_id'], axis = 1, inplace = True)

    return df




