import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
import os

sns.set()

def query_to_df(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database': 'simulator',
                  'user': 'student',
                  'password': 'dpo_python_2020'
}

    df = ph.read_clickhouse(query, connection=connection)
    return df


def test_report(chat=-715060805):
    chat_id = chat
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

    q1 = """select count(DISTINCT user_id) as users,
    countIf(action, action='like') likes,
    countIf(action, action='view') views,
    100 * likes / views as ctr,
    toDate(time) as date
    from simulator_20220520.feed_actions 
    where toDate(time) >= today() - 8 and toDate(time) < today()
    group by date
    order by date DESC  
        """
    data = query_to_df(q1)
    
    today = max(data['date'])
    day_ago = today - pd.DateOffset(days=1)
    week_ago = today - pd.DateOffset(days=7)
    msg = '''📈Отчёт по ЛЕНТЕ за {date}📈
    👤DAU: {users} ({to_users_day_ago:+.2%} к дню назад, {to_users_week_ago:+.2%} к неделе назад)
    ❤Лайки: {likes} ({to_likes_day_ago:+.2%} к дню назад, {to_likes_week_ago:+.2%} к неделе назад)
    👀Просмотры: {views} ({to_views_day_ago:+.2%} к дню назад, {to_views_week_ago:+.2%} к неделе назад)
    🎯CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%} к дню назад, {to_ctr_week_ago:+.2%} к неделе назад)'''
    
    report = msg.format(date=today.date(),
                    users=data[data['date'] == today]['users'].iloc[0],
                    to_users_day_ago=(int(data[data['date'] == today]['users'].iloc[0]) 
                                      - int(data[data['date'] == day_ago]['users'].iloc[0]))
                                      / int(data[data['date'] == day_ago]['users'].iloc[0]),
                    to_users_week_ago=(int(data[data['date'] == today]['users'].iloc[0])
                                      - int(data[data['date'] == week_ago]['users'].iloc[0]))
                                      / int(data[data['date'] == week_ago]['users'].iloc[0]),
                    likes=data[data['date'] == today]['likes'].iloc[0],
                    to_likes_day_ago=(int(data[data['date'] == today]['likes'].iloc[0])
                                      - int(data[data['date'] == day_ago]['likes'].iloc[0]))
                                      / int(data[data['date'] == day_ago]['likes'].iloc[0]),
                    to_likes_week_ago=(int(data[data['date'] == today]['likes'].iloc[0])
                                      - int(data[data['date'] == week_ago]['likes'].iloc[0]))
                                      / int(data[data['date'] == week_ago]['likes'].iloc[0]),
                    views=data[data['date'] == today]['views'].iloc[0],
                    to_views_day_ago=(int(data[data['date'] == today]['views'].iloc[0])
                                      - int(data[data['date'] == day_ago]['views'].iloc[0]))
                                      / int(data[data['date'] == day_ago]['views'].iloc[0]),
                    to_views_week_ago=(int(data[data['date'] == today]['views'].iloc[0])
                                      - int(data[data['date'] == week_ago]['views'].iloc[0]))
                                      / int(data[data['date'] == week_ago]['views'].iloc[0]),
                    ctr=data[data['date'] == today]['ctr'].iloc[0],
                    to_ctr_day_ago=(int(data[data['date'] == today]['ctr'].iloc[0])
                                      - int(data[data['date'] == day_ago]['ctr'].iloc[0]))
                                      / int(data[data['date'] == day_ago]['ctr'].iloc[0]),
                    to_ctr_week_ago=(int(data[data['date'] == today]['ctr'].iloc[0])
                                      - int(data[data['date'] == week_ago]['ctr'].iloc[0]))
                                      / int(data[data['date'] == week_ago]['ctr'].iloc[0])
                   )
    bot.sendMessage(chat_id=chat_id, text=report)

    plt.figure(figsize=(15, 10))
    plt.subplot(4, 1, 1)
    sns.lineplot(data=data, x='date', y='users')
    plt.title('Значения за прошедшие 7 дней')
    plt.ylabel('DAU')

    plt.subplot(4, 1, 2)
    sns.lineplot(data=data, x='date', y='likes')
    plt.ylabel('Лайки')

    plt.subplot(4, 1, 3)
    sns.lineplot(data=data, x='date', y='views')
    plt.ylabel('Просмотры')

    plt.subplot(4, 1, 4)
    sns.lineplot(data=data, x='date', y='ctr')
    plt.ylabel('CTR')

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'report_plot.png'
    plot_object.seek(0)

    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


try:
    test_report()
except Exception as e:
    print(e)
