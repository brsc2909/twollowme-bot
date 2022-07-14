import sqlite3

from twollowme_bot.twitter.client import Friend

FOLLOWERS_DDL = """
CREATE TABLE IF NOT EXISTS friends (
    user_id INTEGER NOT NULL PRIMARY KEY,
    screen_name text,
    name TEXT,
    url TEXT,
    verified BOOLEAN,
    location TEXT,
    friends_count INTEGER, 
    followers_count INTEGER, 
    created_at DATE,
    following BOOLEAN,
    friends BOOLEAN,
    dont_remove BOOLEAN,
    added_by_bot BOOLEAN,
    date_added DATETIME DEFAULT current_timestamp NOT NULL
)
"""

GET_FRIENDS_DML = """
SELECT user_id, screen_name, name, url, verified, location, friends_count, followers_count, created_at, following, friends, dont_remove, added_by_bot
FROM friends
"""

GET_FRIENDS_FOR_REMOVAL_DML = f"""
{GET_FRIENDS_DML}
where dont_remove = 0
AND date_added < date('now','-'||:num_days||' day') 
"""

INSERT_FRIENDS_DML = """
INSERT INTO friends (
    user_id, screen_name, name, url, verified, location, friends_count, followers_count, created_at, following, friends, dont_remove, added_by_bot
) VALUES (
    :user_id, :screen_name, :name, :url, :verified, :location, :friends_count, :followers_count, :created_at, :following, :friends, :dont_remove, :added_by_bot 
) ON CONFLICT (user_id) DO UPDATE SET
    screen_name = :screen_name,
    name = :name,
    url = :url,
    verified = :verified,
    location = :location,
    friends_count = :friends_count,
    followers_count = :followers_count,
    created_at = :created_at,
    following = :following,
    friends = :friends,
    dont_remove = :dont_remove,
    added_by_bot = :added_by_bot
"""

UPDATE_RELATIONHIP_DML = """
UPDATE friends SET 
    following = :following,
    friends = :friends,
WHERE user_id = :user_id;
"""


class Database:
    """Database Manager"""

    conn: sqlite3.Connection

    def __init__(
        self,
        path: str,
        **kwargs,
    ) -> None:
        self.conn = sqlite3.connect(path)

    def __enter__(self):
        return self

    def __exit__(self, exit_type, value, traceback) -> None:
        del exit_type, value, traceback
        self.conn.close()

    def setup(self):
        cursor = self.conn.cursor()
        # make sure tables have been created
        cursor.execute(FOLLOWERS_DDL)

    def get_friends_for_removal(self, num_days):
        cur = self.conn.cursor()
        cur.execute(GET_FRIENDS_FOR_REMOVAL_DML, {"num_days": num_days})

    def update_friends(self, friends: list[Friend]):
        cur = self.conn.cursor()
        cur.executemany(INSERT_FRIENDS_DML, [i.__dict__ for i in friends])

    def update_relationship(self, user_id: int, i_follow: bool, followes_me: bool):
        cur = self.conn.cursor()
        cur.execute(
            UPDATE_RELATIONHIP_DML,
            {"user_id": user_id, "i_follow": i_follow, "followes_me": followes_me},
        )

    def get_friends(self):
        cur = self.conn.cursor()
        cur.execute(GET_FRIENDS_DML)

        return [Friend(*friend) for friend in cur.fetchall()]
