import luigi
import requests
import requests.auth
import pandas as pd
import json
import os


class RedditScoreGenerator(luigi.Task):

    def output(self):
        return luigi.LocalTarget('score.csv')

    def run(self):
        if os.path.exists('score.csv'):
            os.remove('score.csv')
        df = RedditScoreGenerator().calculate_post_score()
        df.to_csv('score.csv', index=False)

    def calculate_number_of_comments(self, subreddit_name, headers):
        sum = 0
        try:
            response = requests.get(f"https://oauth.reddit.com{subreddit_name}comments", headers=headers)
            comment = response.json()
            for c in comment[1]['data']['children']:
                sum += 1
        except:
            print('Error occured')
            return 0
        return sum

    def calculate_comment_score(self, subreddit_name, headers):
        sum = 0
        try:
            response = requests.get(f"https://oauth.reddit.com{subreddit_name}comments", headers=headers)
            comment = response.json()
            for c in comment[1]['data']['children']:
                sum += int(c['data']['score'])
        except:
            print('Error occured')
            return 0
        return sum

    def calculate_post_score(self):
        data = json.load(open('credentials.json'))
        client_auth = requests.auth.HTTPBasicAuth(data['app_id'], data['secret'])
        post_data = {"grant_type": "password", "username": data['username'], "password": data['password']}
        headers = {"User-Agent": "ChangeMeClient/0.1 by ankit_chobdar"}

        response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth,
                                 data=post_data, headers=headers)

        output = response.json()

        headers = {"Authorization": f"bearer {output['access_token']}",
                   "User-Agent": "ChangeMeClient/0.1 by ankit_chobdar"}

        subreddit = '/hot/?t=hour'

        response = requests.get(f"https://oauth.reddit.com/{subreddit}", headers=headers)
        final_output = response.json()
        result = final_output['data']['children']

        lst = []
        for i in result:
            s = pd.Series({'title': i['data']['title'],
                           'subreddit': i['data']['subreddit'],
                           'link': i['data']['permalink'],
                           'num_comments': i['data']['num_comments'],
                           'score': i['data']['score']
                           })
            lst.append(s)

        df = pd.DataFrame(lst)

        df['comment_points'] = df['link'].apply(self.calculate_comment_score, headers=headers)
        df['number_of_comments'] = df['link'].apply(self.calculate_number_of_comments, headers=headers)
        df = df.assign(post_score=lambda x: x['comment_points'] / x['number_of_comments']).fillna(0)

        return df[['title', 'subreddit', 'post_score']]


if __name__ == '__main__':
    df = RedditScoreGenerator().calculate_post_score()
    # luigi.build(['RedditScoreGenerator'], local_scheduler=True)
