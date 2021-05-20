from google.cloud import storage


class BucketConfig:

    def __init__(self, awacslogger) -> None:
        self.__storage_client = storage.Client()
        #self.__storage_client = storage.Client.from_service_account_json(
        #    'C:/Users/Shubham Snehi/Downloads/awacs-dev-3543bb21996e.json')
        self.__awacslogger = awacslogger

    # Cloud Storage Bucket Methods
    # Bucket Connection
    def getbucketconn(self, bucketname):
        try:
            bucket = self.__storage_client.get_bucket(bucketname)
            self.__awacslogger.info("Bucket Connection Successful")
            return bucket
        except Exception as e:
            self.__awacslogger.error("Unable to connect bucket :" + str(e))
            print("Unable to connect bucket :" + str(e))
            exit(-1)
