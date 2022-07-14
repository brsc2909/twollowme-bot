from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Literal

from twarc.client2 import Twarc2
from tweepy import API, Client, Cursor, OAuth1UserHandler
from tweepy.models import User

Lang = Literal["en", "gb"]
ResultType = Literal["mixed", "recent", "popular"]


class SearchMethod(Enum):
    ALL = "all"
    RECENT = "recent"


@dataclass
class Friend:
    """Twitter Friend Object"""

    user_id: int
    screen_name: str
    name: str
    url: str
    verified: bool
    location: str
    friends_count: int
    followers_count: int
    created_at: datetime
    following: bool
    friends: bool
    dont_remove: bool
    added_by_bot: bool
    date_added: datetime


class TwitterV1Api(API):
    """Twitter API Client V1.1"""

    def __init__(self, **kwargs):
        kwargs.pop("bearer_token")
        auth = OAuth1UserHandler(**kwargs)
        super().__init__(auth, wait_on_rate_limit=True)

    def search(self, query: str, lang: Lang = None, result_type: ResultType = "mixed"):
        return Cursor(
            super().search_tweets, q=query, lang=lang, result_type=result_type
        ).pages()

    def get_friends(self, **kwargs):
        return Cursor(
            super().get_friends, count=200, skip_status=True, **kwargs
        ).pages()

    def follow(self, user_id: int):
        return self.create_friendship(user_id=user_id, follow=True)

    def unfollow(self, user_id: int):
        return self.destroy_friendship(user_id=user_id)


def parse_user(user: User) -> Friend:
    return Friend(
        user_id=user.id,
        screen_name=user.screen_name,
        name=user.name,
        url=user.url,
        verified=user.verified,
        location=user.location,
        friends_count=user.friends_count,
        followers_count=user.followers_count,
        created_at=user.created_at,
        following=user.following,
        friends=None,
        dont_remove=None,
        added_by_bot=None,
        date_added=None,
    )


class TwitterAPI(Twarc2, Client):
    """Twitter API Client V2"""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)  # this calls all constructors up to Foo
        super(Twarc2, self).__init__(**kwargs)

    def search_recent(self):
        pass

    def search(
        self,
        query: str,
        start: datetime = datetime.now() - timedelta(days=7),
        end: datetime = datetime.now() - timedelta(days=1),
        limit: int = None,
        method: SearchMethod = SearchMethod.RECENT,
    ):
        max_batch = 500 if method == SearchMethod.ALL else 100

        search_results = self.search_all(
            query=query,
            start_time=start,
            end_time=end,
            max_results=max_batch if not limit or limit > max_batch else limit,
        )

        for page in search_results:
            tweets = page["data"]
            users = page["includes"]["users"]
            places = {
                place["id"]: place["full_name"]
                for place in page["includes"].get("places", [])
            }

            yield tweets, users, places
