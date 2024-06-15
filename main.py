from azure.storage.blob import BlobServiceClient, BlobClient
from pymongo import MongoClient
import os
from dotenv import load_dotenv
# import pathlib
from yaspin import yaspin
from yaspin.spinners import Spinners
load_dotenv()

AUCTION_LOT_NUMBER = 1616

# construct mongoDB client
# ssl hand shake error because ip not whitelisted
client = MongoClient(
    os.getenv('DATABASE_URL'),
    maxPoolSize=1
)

# database handle
db = client['CCPD']
auction_collection = db['AuctionHistory']

# blob service client object
azure_blob_client = BlobServiceClient.from_connection_string(os.getenv('SAS_KEY'))

# container client
product_image_container_client = azure_blob_client.get_container_client('product-image')

@yaspin(Spinners.earth, text="Downloading Images...")
def main():
    # find auction record by lot number, get the items list
    res = auction_collection.find_one({'lot': AUCTION_LOT_NUMBER})
    itemArr = res['itemsArr']
    auctionLot = res['lot']
    
    # create folder for that lot
    folder_name = f'Lot_{auctionLot}_Images'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    downloadCount = 0
    skuCount = 0
    # loop through all auction items
    for item in itemArr:
        lot = item['lot']
        sku_filter = f"sku = '{item['sku']}'"
        blob_list = product_image_container_client.find_blobs_by_tags(filter_expression=sku_filter)
        sorted_blobs = sorted(blob_list, key=lambda blob: blob.name, reverse=True)
        if sorted_blobs:
            skuCount += 1
        
        # download and rename
        flag = 1
        for blob in sorted_blobs: 
            blob_client = product_image_container_client.get_blob_client(blob.name)
            image_filename = f'{folder_name}/{lot}_{flag}.jpg'
            if not os.path.exists(image_filename):
                with open(image_filename, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())
                    downloadCount += 1
            flag += 1
    
    print(f'\nDownloaded {downloadCount} Images for {skuCount} SKUs From Auction {AUCTION_LOT_NUMBER}')

if __name__ == '__main__':
    main()