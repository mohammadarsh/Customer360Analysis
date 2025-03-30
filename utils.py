from datetime import  datetime,timedelta

def generate_yesterday_date(adhoc_date):
    '''
    this method will gnerate date as is else genrate a date
    :param adhoc_date: user provide data
    :return: date in string format (YYYY-MM-DD)
    '''
    if adhoc_date:
        yesterday_date = adhoc_date
    else:
        yesterday_date = datetime.now() - timedelta(days=1)
        yesterday_date = yesterday_date.strftime("%Y-%m-%d")

    return yesterday_date
