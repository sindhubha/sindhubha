import datetime
from dateutil import parser

async def parse_date(from_date):
    date_from = from_date.split("-")
    if len(date_from[0]) <= 2:
        if int(date_from[0]) > 12:
            from_date = parser.parse(from_date).strftime("%Y-%m-%d %H:%M:%S")
        else:
            from_date = parser.parse(from_date).strftime("%Y-%d-%m %H:%M:%S")
        from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    else:
        from_date = parser.parse(from_date).strftime("%Y-%m-%d %H:%M:%S")
        from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    return from_date 