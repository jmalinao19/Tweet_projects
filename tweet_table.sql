DROP TABLE IF EXISTS tweets;

CREATE TABLE tweets (text varchar(200) PRIMARY KEY NOT NULL,
    Topic varchar(60),
    country varchar(60),
    lang varchar(60),
    user_name varchar(60),
    screen_name varchar(60),
    coord_lat varchar(30),
    coord_long varchar(30),
    location varchar(60),
    retweet_count integer,
    follower_count integer,
    favorite_count integer,
    friend_count integer,
    texts varchar(300),
    sentiment varchar(60),
    sentiment_score float
);

ALTER TABLE tweets ADD COLUMN create_at TIMESTAMP;
ALTER TABLE tweets ALTER COLUMN created_at Set DEFAULT now();
DROP TABLE IF EXISTS tweet_words;

CREATE TABLE tweet_words( word varchar(40) PRIMARY KEY NOT NULL,
    topic varchar(40),
    count integer,
    word_sentiment varchar(50)
);