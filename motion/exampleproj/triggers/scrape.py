from datetime import datetime

import requests

import motion


class ScrapeWikipedia(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                relation="",
                key="* * * * *",
                infer=self.scrape,
                fit=None,
            )
        ]

    def setUp(self, cursor):
        scraping_params = {
            "format": "json",
            "rcprop": "title|ids|user|userid|comment|timestamp",
            "list": "recentchanges",
            "action": "query",
            "rclimit": "100",
        }

        return {
            "url": "https://en.wikipedia.org/w/api.php",
            "scraping_params": scraping_params,
        }

    def scrape(self, cursor, triggered_by):
        try:
            S = requests.Session()
            R = S.get(url=self.state["url"], params=self.state["scraping_params"])
            data = R.json()

            recent_changes = data["query"]["recentchanges"]
            for rc in recent_changes:
                if rc["type"] == "edit":
                    title = rc["title"]
                    pageid = str(rc["pageid"])
                    user = rc["user"]
                    userid = str(rc["userid"])
                    comment = rc["comment"]
                    timestamp = datetime.strptime(rc["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

                    record = {
                        "title": title,
                        "pageid": pageid,
                        "user": user,
                        "userid": userid,
                        "comment": comment,
                        "edited_timestamp": timestamp,
                    }

                    cursor.set(
                        relation="WikiEdit",
                        identifier="",
                        key_values=record,
                    )

        except Exception as e:
            print(e)
