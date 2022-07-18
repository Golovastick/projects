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


def test_report(chat=None):
    chat_id = chat or -715060805 # Чат курсов
    bot = telegram.Bot(token='5523468390:AAH2--fapGulRM8nyxmFnSf15lSRuRkNLR4')

    # Считаем данные по новостям и сообщениям
    q1 = """
    with data as (
    select user_id, toDate(time) as time from simulator_20220520.feed_actions 
    where toDate(time) >= today() - 8 and toDate(time) < today()
    union all
    select user_id, toDate(time)::date as time from simulator_20220520.message_actions 
    where toDate(time) >= today() - 8 and toDate(time) < today()
    )

    select count(distinct user_id) as dau_app, time from data
    group by time
    order by time
    """

    df_app = query_to_df(q1)
    
    q2 = """
    select count(distinct user_id) as dau_feed, countIf(user_id, action = 'like') as likes, countIf(user_id, action = 'view') as views,
    100 * likes / views as ctr, toDate(time) as time 
    from simulator_20220520.feed_actions 
    where toDate(time) >= today() - 8 and toDate(time) < today()
    group by time
    order by time
    """
    
    df_fa = query_to_df(q2)
    
    q3 = """
    select count(distinct user_id) as dau_msg, count(user_id) as messages,
    toDate(time) as time 
    from simulator_20220520.message_actions 
    where toDate(time) >= today() - 8 and toDate(time) < today()
    group by time
    order by time
    """
    
    df_ma = query_to_df(q3)
    
    df = df_app.merge(df_fa, on = 'time', how = 'left')
    df = df.merge(df_ma, on = 'time', how = 'left')
    
    # Расчитываем метрики
    df['actions'] = df['likes'] + df['views'] + df['messages']
    df['actions_per_user'] = round(df['actions'] / df['dau_app'])
    df['dau_both'] = df['dau_feed'] + df['dau_msg'] - df['dau_app']

    # Сохраняем даты отчётного дня, предыдущего и неделю назад
    today = max(df['time'])
    day_ago = today - pd.DateOffset(days=1)
    week_ago = today - pd.DateOffset(days=7)

    # Формируем сообщение
    msg = '''📈Единый отчёт за {date}📈
    👤🌏DAU ленты новостей: {dau_feed} ({to_dau_feed_day_ago:+.2%} к дню назад, {to_dau_feed_week_ago:+.2%} к неделе назад)
    👤📨DAU сервиса сообщений: {dau_msg} ({to_dau_msg_day_ago:+.2%} к дню назад, {to_dau_msg_week_ago:+.2%} к неделе назад)
    👤📱DAU приложения: {dau_app} ({to_dau_app_day_ago:+.2%} к дню назад, {to_dau_app_week_ago:+.2%} к неделе назад)
    👤🌏📨DAU обоих сервисов: {dau_both} ({to_dau_both_day_ago:+.2%} к дню назад, {to_dau_both_week_ago:+.2%} к неделе назад)
    🎯CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%} к дню назад, {to_ctr_week_ago:+.2%} к неделе назад)
    🤳Действий на одного пользователя: {actions_per_user} ({to_actions_per_user_day_ago:+.2%} к дню назад, {to_actions_per_user_week_ago:+.2%} к неделе назад)'''
    
    report = msg.format(date=today.date(),
                    dau_feed=df[df['time'] == today]['dau_feed'].iloc[0],
                    to_dau_feed_day_ago=(int(df[df['time'] == today]['dau_feed'].iloc[0]) 
                                      - int(df[df['time'] == day_ago]['dau_feed'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['dau_feed'].iloc[0]),
                    to_dau_feed_week_ago=(int(df[df['time'] == today]['dau_feed'].iloc[0])
                                      - int(df[df['time'] == week_ago]['dau_feed'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['dau_feed'].iloc[0]),
                    dau_msg=df[df['time'] == today]['dau_msg'].iloc[0],
                    to_dau_msg_day_ago=(int(df[df['time'] == today]['dau_msg'].iloc[0]) 
                                      - int(df[df['time'] == day_ago]['dau_msg'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['dau_msg'].iloc[0]),
                    to_dau_msg_week_ago=(int(df[df['time'] == today]['dau_msg'].iloc[0])
                                      - int(df[df['time'] == week_ago]['dau_msg'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['dau_msg'].iloc[0]),
                    dau_app=df[df['time'] == today]['dau_app'].iloc[0],
                    to_dau_app_day_ago=(int(df[df['time'] == today]['dau_app'].iloc[0]) 
                                      - int(df[df['time'] == day_ago]['dau_app'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['dau_app'].iloc[0]),
                    to_dau_app_week_ago=(int(df[df['time'] == today]['dau_app'].iloc[0])
                                      - int(df[df['time'] == week_ago]['dau_app'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['dau_app'].iloc[0]),
                    dau_both=df[df['time'] == today]['dau_both'].iloc[0],
                    to_dau_both_day_ago=(int(df[df['time'] == today]['dau_both'].iloc[0]) 
                                      - int(df[df['time'] == day_ago]['dau_both'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['dau_both'].iloc[0]),
                    to_dau_both_week_ago=(int(df[df['time'] == today]['dau_both'].iloc[0])
                                      - int(df[df['time'] == week_ago]['dau_both'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['dau_both'].iloc[0]),
                    ctr=df[df['time'] == today]['ctr'].iloc[0],
                    to_ctr_day_ago=(int(df[df['time'] == today]['ctr'].iloc[0])
                                      - int(df[df['time'] == day_ago]['ctr'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['ctr'].iloc[0]),
                    to_ctr_week_ago=(int(df[df['time'] == today]['ctr'].iloc[0])
                                      - int(df[df['time'] == week_ago]['ctr'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['ctr'].iloc[0]),
                    actions_per_user=df[df['time'] == today]['actions_per_user'].iloc[0],
                    to_actions_per_user_day_ago=(int(df[df['time'] == today]['actions_per_user'].iloc[0])
                                      - int(df[df['time'] == day_ago]['actions_per_user'].iloc[0]))
                                      / int(df[df['time'] == day_ago]['actions_per_user'].iloc[0]),
                    to_actions_per_user_week_ago=(int(df[df['time'] == today]['actions_per_user'].iloc[0])
                                      - int(df[df['time'] == week_ago]['actions_per_user'].iloc[0]))
                                      / int(df[df['time'] == week_ago]['actions_per_user'].iloc[0])                    
                   )

    bot.sendMessage(chat_id=chat_id, text=report)

    # Формируем графики
    fig, axs = plt.subplots(nrows=6, ncols=1, figsize=(10, 24))

    axs[0].set_title("DAU ленты новостей")
    axs[0].plot(df['time'], df['dau_feed'])
    axs[0].set_xlabel("Дата")
    axs[0].set_ylabel("Количество")

    axs[1].set_title("DAU сервиса сообщений")
    axs[1].plot(df['time'], df['dau_msg'])
    axs[1].set_xlabel("Дата")
    axs[1].set_ylabel("Количество")

    axs[2].set_title("DAU приложения")
    axs[2].plot(df['time'], df['dau_app'])
    axs[2].set_xlabel("Дата")
    axs[2].set_ylabel("Количество")

    axs[3].set_title("DAU обоих сервисов")
    axs[3].plot(df['time'], df['dau_both'])
    axs[3].set_xlabel("Дата")
    axs[3].set_ylabel("Количество")

    axs[4].set_title("CTR")
    axs[4].plot(df['time'], df['ctr'])
    axs[4].set_xlabel("Дата")
    axs[4].set_ylabel("Значение в %")

    axs[5].set_title("Действий на одного пользователя")
    axs[5].plot(df['time'], df['actions_per_user'])
    axs[5].set_xlabel("Дата")
    axs[5].set_ylabel("Количество")

    fig.tight_layout()

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