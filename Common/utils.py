from datetime import datetime

@staticmethod
def print_debug_msg(msg: str):
    """
    Adds a timestamp to a printed message

    :param msg: the message that gets appended onto a timestamp and output to console
    :return: None
    """

    # get the timestamp
    now: datetime = datetime.now()

    # output the text
    print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')
