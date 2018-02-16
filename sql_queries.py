legislators_sql = """
    WITH officials_stats AS
        (
        SELECT l.party,
            l.id,
            l.gender,
            l.first_name || ' ' || l.last_name as name,
            l.religion,
            tp.id as twitter_id,
            tp.location,
            tp.followers_count,
            tp.favourites_count,
            tp.statuses_count,
            tp.friends_count
        FROM legislators l
        LEFT JOIN social s
            ON l.id = s.legislator_id
        LEFT JOIN twitter_profiles tp
            ON tp.id = s.twitter_id
        ),
        
        tweet_count AS
        (
            SELECT t.twitter_id,
                count(distinct(t.id)) as tweet_count,
                sum(favorite_count) as tweet_fav_count,
                sum(retweet_count) as retweet_count
            FROM tweets t
            GROUP BY 1
        )
    
    SELECT o.*, t.tweet_count, t.tweet_fav_count, t.retweet_count
    FROM officials_stats as o
    LEFT JOIN tweet_count as t
        ON o.twitter_id = t.twitter_id;
"""

tweets_sql = """
    SELECT l.first_name || ' ' || l.last_name as name,
        l.party,
        l.gender,
        t.id,
        t.twitter_screen_name,
        t.created_at,
        t.hashtags,
        t.text,
        t.text_length,
        t.user_mentions,
        t.favorite_count,
        t.retweet_count,
        t.media_type,
        t.time_collected,
        MAX(u.followers_count) as user_followers
    FROM tweets t
    LEFT JOIN social s
        ON lower(t.twitter_screen_name) = lower(s.twitter_screen_name)
    LEFT JOIN legislators l
        ON s.legislator_id = l.id
    LEFT JOIN user_profile_log u
        ON (t.twitter_screen_name = u.screen_name AND DATE(t.time_collected) = DATE(u.time_collected))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;
"""

