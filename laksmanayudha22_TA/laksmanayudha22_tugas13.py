import sqlite3
import tweepy
from datetime import datetime
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

twitter_api = {
    'consumer_key':"yVeXYxgfpdMItTTHB7M02BiQz",
    'consumer_secret':"d5uit1opigAvwDXdhwg6697WZmjvbb8pTq9UOLH23NInCBksM7",
    'access_token':"1324197224310480899-PZgznTyGbXQxbgqQSwNps8ks7ddXwV",
    'access_token_secret':"56HTxzADHWXX9OsQ60wHgGzmJcCmx2BgME5dUQ5S3iVGO"
}
query = 'omnibus law'
database = "laksmanayudha22_final.db"
jumlah_tweet = 200

class DataHandler :
    
    scraping_id = 0
    # date_since = datetime.today().strftime('%Y-%m-%d')
    date_since = ""
    
    def __init__(self, database):
        self.tweet_container = []
        self.user_container = []
        self.database = database
    def save_sql(self):
        
        # cek status dan ganti status
        active = self.get_active()
        #print(active)
        if active == None or active[1] != DataHandler.date_since :
            self.swap_active()
            # insert lastscrapping
            connection = sqlite3.connect(self.database)
            cursor = connection.cursor()
            query = """INSERT INTO lastscraping VALUES (?, ?, ?);"""
            lastscrap = (DataHandler.scraping_id, DataHandler.date_since, 1)
            cursor.execute(query, lastscrap)
            connection.commit()
            cursor.close()
            connection.close()
        
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
  
        # insert user
        query = """INSERT INTO user VALUES  (?, ?, ?, ?, ?, ?, ?, ?);"""

        for user in self.user_container:
            if(self.get_userid(user[0])) :
                cursor.execute(query,  user)
                connection.commit()
       
        # insert tweet
        query = """INSERT INTO tweet ("tweetid", "userid", "createddate", "tweet", "scraping_id") VALUES (?, ?, ?, ?, ?);"""
        for tweet in self.tweet_container:
            print(tweet)
            if(self.get_tweetid(tweet[0])) :
                cursor.execute(query,  tweet)
                connection.commit()
        
        print("\n\n")
        self.tweet_container = []
        self.user_container = []
        cursor.close()
        connection.close()
        
    def get_data(self, twitter_api, query, jumlah_tweet):
        consumer_key        = twitter_api['consumer_key']
        consumer_secret     = twitter_api['consumer_secret']
        access_token        = twitter_api['access_token']
        access_token_secret = twitter_api['access_token_secret']
        
        # autentikasi
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        
        # (cyberbullying) (since:2020-11-20  until:2020-11-21) (-filter:retweets)
        date = DataHandler.date_since.split("-")
        year = int(date[0])
        month = int(date[1])
        day = int(date[2])
        before = datetime(year, month, day, 00, 00, 00).strftime('%Y-%m-%d')
        after = datetime(year, month, day+1, 23, 59, 00).strftime('%Y-%m-%d')

        retweet_filter = '-filter:retweets'
        since_date = "since:" + before
        until_date = "until:" + after
        new_query = "(" + query + ") " + "(" + retweet_filter + ") " + "(" + since_date + " "+ until_date + ")"

        # mengambil tweet
        
        tweets = api.search(q=new_query, lang="id", count=jumlah_tweet, tweet_mode="extended")
        #print(tweets)
        # ambil data
        
        active = self.get_active()
        #print(active)
        if active != None :
            if active[1] == DataHandler.date_since :
                DataHandler.scraping_id = active[0]
            else :
                DataHandler.scraping_id = active[0] + 1
        else :
            DataHandler.scraping_id += 1
  
        for tweet in tweets:
            
            # tweet
            tweet_id = tweet.id
            user_id = tweet.user.id
            createddate = tweet.created_at
            tweet_text = tweet.full_text
            self.tweet_container.append((tweet_id, user_id, createddate, tweet_text, DataHandler.scraping_id))
            
            #user
            user_name = tweet.user.name
            screenname = tweet.user.screen_name
            location = tweet.user.location
            acccreated = tweet.user.created_at
            follower = tweet.user.followers_count
            friend = tweet.user.friends_count
            verified = tweet.user.verified
            self.user_container.append((user_id, user_name, screenname, location, acccreated, follower, friend, verified))
        
        print(len(self.tweet_container))
        
    def clean_data(self):

        scrap_id = self.get_active()[0]

        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        # ambil semua data tweet
        query = "SELECT tweetid, tweet FROM tweet WHERE scraping_id = " + str(scrap_id)
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchall()
        cursor.close()
        connection.close()

        # pisahkan tweet dan tweet id
        tweets = []
        tweet_ids = []
        for tweet in data :
            tweets.append(tweet[1])
            tweet_ids.append(tweet[0])

        # bersihkan data
        #case folding
        tweets = [tweet.lower() for tweet in tweets] 
        tweets = [' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) for tweet in tweets]

        # #stopwords
        stop_words = set(stopwords.words('indonesian'))
        clean_tweets = []
        for tweet in tweets:
            word_tokens = word_tokenize(tweet) 
            new_tweet = ' '.join([w for w in word_tokens if not w in stop_words])
            clean_tweets.append(new_tweet)
        
        #stemming
        factory = StemmerFactory()
        stemmer = factory.create_stemmer()
        clean_tweets_2 = []
        for tweet in clean_tweets:
            new_tweet = stemmer.stem(tweet)
            clean_tweets_2.append(new_tweet)
        
        #update ke database
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        update = list(zip(clean_tweets_2, tweet_ids))
        query = """UPDATE tweet SET cleantweet = (?) WHERE tweetid = (?);"""
        cursor.executemany(query, update)
        connection.commit()
        cursor.close()
        connection.close()

    def reset_sraping_id(self):
        pass

    def get_scrapid(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """SELECT scraping_id FROM lastscraping WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        return data
    def get_active (self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """SELECT * FROM lastscraping WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        return data

    def swap_active(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """UPDATE lastscraping SET status = 0 WHERE status = 1;"""
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    def get_userid(self, userid):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = "SELECT userid FROM user WHERE userid = " + str(userid)
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if data == None :
            return True
        else:
            return False

    def get_tweetid(self, tweetid):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = "SELECT tweetid FROM tweet WHERE tweetid = " + str(tweetid)
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if data == None :
            return True
        else:
            return False

    def delete_all_table(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        
        # ambil data
        query = """DELETE FROM tweet;"""
        cursor.execute(query)
        connection.commit()
        query = """DELETE FROM user;"""
        cursor.execute(query)
        connection.commit()
        query = """DELETE FROM lastscraping;"""
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
    
        

data_handler = DataHandler(database)

# data_handler.delete_all_table()

# DataHandler.date_since = "2020-11-18"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-19"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-20"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-21"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-22"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-23"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-24"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-25"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-26"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-27"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()

# DataHandler.date_since = "2020-11-28"
# data_handler.get_data(twitter_api, query, jumlah_tweet)
# data_handler.save_sql()
# data_handler.clean_data()
