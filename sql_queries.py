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
