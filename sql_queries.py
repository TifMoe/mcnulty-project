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
    
    SELECT *
    FROM officials_stats
    LEFT JOIN tweet_count 
        ON officials_stats.twitter_id = tweet_count.twitter_id;
"""
