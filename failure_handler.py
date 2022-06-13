from ib_insync import *
import datetime as dt
from time import sleep
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# not recommended, but for simplicity
GMAIL_USER = "garylam@lamindexinvest.com"
GMAIL_APP_PASSWORD = "Lamindexinvest123!"


def send_email(text_message):
    # setup the MIME
    message = MIMEMultipart()
    message['From'] = "TWL connection inspector"
    message['To'] = GMAIL_USER
    message['Subject'] = "TWS lost connection"
    message.attach(MIMEText(text_message))
    text = message.as_string()
    try:
        session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
        session.ehlo()
        session.starttls()  # enable security
        session.ehlo()
        session.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        session.sendmail(GMAIL_USER, GMAIL_USER, text)
        session.quit()
        print("Email sent !")
    except Exception as e:
        print(f"Something went wrong from the email server, error message: {e}")


def connection_handler(func):
    def inner(*args):  # to grab all parameters in "func"
        try:
            return func(*args)  # try execute the program and see if there is any problem
        except Exception as e:
            # send a notification to the user about the disruption
            error_time = dt.datetime.now()
            message = f"{func.__name__} was disrupted at {error_time}.\nError message: {e}\nTrying to run the function again after 30 seconds..."
            """
            write the message to a log ???
            """
            print(message)
            send_email(message)
            sleep(10)
            return inner(*args)  # recursive call

    return inner


# for handling automatic closure of TWS
def connect_tws(ib_instance):
    ib_instance.sleep(0)
    try:
        if not ib_instance.isConnected():
            print(f"TWS is not connected to the client at {dt.datetime.now()}, trying to reconnect after 5 seconds...")
            sleep(5)
            ib_instance.disconnect()
            ib_instance.connect('127.0.0.1', 7497, clientId=1)
            print("TWS re-connected !")
    except Exception as e:
        print("Exception or error:", e)
        connect_tws(ib_instance)


def main():
    # ib = IB()
    # connection_checker = tws_connection(ib)
    # connection_checker.check_comnection(60)
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    while True:
        connect_tws(ib)
        sleep(5)


if __name__ == "__main__":
    main()
