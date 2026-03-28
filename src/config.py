import os
from dotenv import load_dotenv
load_dotenv()

#=====================================================#
#SSI_API_KEY_CONFIG
auth_type = 'Bearer'
consumerID = os.getenv("CONSUMER_ID")
consumerSecret = os.getenv("CONSUMER_SECRET")

url = 'https://fc-data.ssi.com.vn/'
stream_url = 'https://fc-datahub.ssi.com.vn/'
# =====================================================#
# OTHER BROKER CONFIG

#=====================================================#
#DATABASE_KEY_CONFIG
DB_URI = os.getenv("DB_URI")
#=====================================================#