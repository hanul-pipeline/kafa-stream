from configparser import ConfigParser
import os

# create config
config = ConfigParser()
config.read(f"{os.path.dirname(os.path.abspath(__file__))}/../config/config.ini")

access_token = config.get("line", "token")

def send_line_noti_thread(message:str):
    from threading import Thread
    
    # define single thread func
    def send_line_noti(access_token:str, message:str):
        import requests

        try:
            url = "https://notify-api.line.me/api/notify"
            headers = {"Authorization": f"Bearer {access_token}"}
            data = {"message" : message}
            response = requests.post(url, headers=headers, data=data)
            print(response) # <--- fix
        
        except Exception as E:
            status = E
            print(status) # <--- fix
    
    # run thread
    thread = Thread(target=send_line_noti, args=(access_token, message))
    thread.start()
    