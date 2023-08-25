#      Makhachkala      #
## Vega Time Corrector ##
### Magomedov Magomed ###

import json

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

from base64 import b64encode
from base64 import b64decode

import random

from requests import get
from requests import post

from time import sleep
from time import time

# MQTT (логин, пароль, IP сервера, порт, время жизни соединения):
# Первые 3 пункта указываем свои
mqtt_name = ''
mqtt_pass = ''
mqtt_server = ''
mqtt_port = 1883
mqtt_keep = 60

# приложение (идентификатор) и устройство (его deviceEUI)
AppID = 'Your App ID'   #Идентификатор приложения, с которым работаем.
deviceEUIAll = '+'      #Все устройства в выбранном приложении
deviceEUI = '0000000000000000'

# флаг предыдущего сообщения PUBLISH
# с отключением подтверждения пакетов
confirmed = False

# ПАКЕТ КОРРЕКЦИИ ВРЕМЕНИ
time_correct = '{"confirmed":false,"fPort":4,"data":"***"}'

# функция возвращает кодированную в base64 байт-строку
# пакета корректировки времени ('FF' + 8 байт дельты в little-endian, со знаком)
def CreateTimeCorrectionPacket(utc_delta):
        tc_bytes = utc_delta.to_bytes(8, byteorder = 'little', signed = True)
        return b64encode(0xff.to_bytes(1, 'little') + tc_bytes)

# функция возвращает время UTC0
# на внутренних часах устройства (счётчика)
def TimeOnDevice(time_pay, device_name):
    if device_name == 'topaz':
        utc_on_device = int(time_pay[16:18]+time_pay[14:16]+time_pay[12:14]+time_pay[10:12], 16)
        return utc_on_device
    elif device_name == 'sgve':
        utc_on_device = int(time_pay[16:18]+time_pay[14:16]+time_pay[12:14]+time_pay[10:12], 16)
        return utc_on_device
    else:
        return False

# проверка payload на признак
# пакета коррекции времени
# если длина == 10 символам (5 байтам)
# и первый байт == 'ff' либо 'FF',
# то это пакет запроса коррекции времени
def TimeCorrectPay(payload):
        if len(payload) == 10 and\
           (payload.find('ff') == 0 or payload.find('FF') == 0):
                return True
        else:
                return False

# функция, вызываемая, когда клиент
# получает CONNACK-ответ от сервера (при успешном соединении)
def on_connect(client, userdata, flags, rc):
    print("Connected to server, result code " + str(rc), end='\n\n')
    
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # Подписка в функции on_connect() означает, что, если мы потеряем соединение
    # и снова подключимся, то подписки будут возобновлены.
    if rc == 0:
        client.subscribe('application/'+AppID+'/device/'+deviceEUIAll+'/event/up')
        print('*' * len('Vega time corrector started ...'))
        print('Vega time corrector started ...')
        print('*' * len('Vega time corrector started ...') + '\n\n')
    else:
        print('*' * len('Connection error !'))
        print('Connection error !')
        print('*' * len('Connection error !') + '\n\n')

event_global = False

# функция, вызываемая при получении сообщения PUBLISH
# от сервера (при получении сообщения для подписанного топика)
def on_message(client, userdata, msg):
    global payload, deviceEUI, real_time, dev_time, time_delta
    if json.loads(msg.payload)['fPort'] == 4:
        payload = msg.payload
        real_time = time()
        dev_time = int(b64decode(json.loads(msg.payload)['data'])[1:5].hex()[6:8]+\
                   b64decode(json.loads(msg.payload)['data'])[1:5].hex()[4:6]+\
                   b64decode(json.loads(msg.payload)['data'])[1:5].hex()[2:4]+\
                   b64decode(json.loads(msg.payload)['data'])[1:5].hex()[0:2], 16)
        time_delta = int(real_time - dev_time)
        if abs(time_delta) > 60:
            deviceEUI = b64decode(json.loads(msg.payload)['devEUI']).hex()
            client.publish(f'application/{AppID}/device/{deviceEUI}/command/down',
                           time_correct.replace('***', CreateTimeCorrectionPacket(time_delta).decode()), qos=0)
            print('Device time on', b64decode(json.loads(msg.payload)['devEUI']).hex(), '('+str(int(dev_time-real_time))+' sec.)', 'will be adjusted !', end = '\n\n')
        else:
            print('Device time on', b64decode(json.loads(msg.payload)['devEUI']).hex(), '- Ok !', '('+str(int(dev_time-real_time))+' sec.)', end = '\n\n')

# функция вызываемая при публикации
# сообщения для топика down (сообщение к устройству)
def on_publish(client, userdata, mid):
    print('*' * len('Message published !'))
    print('Message published !')
    print('*' * len('Message published !') + '\n')

# функция вызывается при разъединении с сервером
def on_disconnect(client, userdata, rc):
    print('*' * len('Client disconnected !'))
    print('Client disconnected !')
    print('*' * len('Client disconnected !') + '\n')
    print('*' * len('The end !'))
    print('The end !')
    print('*' * len('The end !') + '\n')

## настройка MQTT перед использованием
client = mqtt.Client(transport = "tcp")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_publish = on_publish

client.username_pw_set(mqtt_name, mqtt_pass)
client.connect(mqtt_server, mqtt_port, mqtt_keep)

client.loop_forever()
