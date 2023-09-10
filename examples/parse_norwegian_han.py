from datetime import datetime
import pprint
import paho.mqtt.client as pahoclient
import os
projPath=os.getcwd()
import sys
sys.path.append(projPath)
from queue import Queue
from dlms_cosem.cosem import Obis
from dlms_cosem.hdlc import frames
from dlms_cosem.protocol import xdlms
from dlms_cosem.time import datetime_from_bytes
from dlms_cosem.utils import parse_as_dlms_data
import time
import json

# 3-phase
hdlc_data_hex = (
    "7ea2434108831385ebe6e7000f4000000000011b020209060000010000ff090c07e30c1001073b28ff"
    "8000ff020309060100010700ff060000046202020f00161b020309060100020700ff06000000000202"
    "0f00161b020309060100030700ff06000005e302020f00161d020309060100040700ff060000000002"
    "020f00161d0203090601001f0700ff10000002020fff1621020309060100330700ff10004b02020fff"
    "1621020309060100470700ff10000002020fff1621020309060100200700ff12090302020fff162302"
    "0309060100340700ff1209c302020fff1623020309060100480700ff12090402020fff162302030906"
    "0100150700ff060000000002020f00161b020309060100160700ff060000000002020f00161b020309"
    "060100170700ff060000000002020f00161d020309060100180700ff060000000002020f00161d0203"
    "09060100290700ff060000046202020f00161b0203090601002a0700ff060000000002020f00161b02"
    "03090601002b0700ff06000005e202020f00161d0203090601002c0700ff060000000002020f00161d"
    "0203090601003d0700ff060000000002020f00161b0203090601003e0700ff060000000002020f0016"
    "1b0203090601003f0700ff060000000002020f00161d020309060100400700ff060000000002020f00"
    "161d020309060100010800ff060099598602020f00161e020309060100020800ff060000000802020f"
    "00161e020309060100030800ff060064ed4b02020f001620020309060100040800ff06000000050202"
    "0f001620be407e"
)
                 
hdlc_data_hex2 = "7EA0E22B2113239AE6E7000F000000000C07E6011801123A32FF80000002190A0E4B616D73747275705F563030303109060101000005FF0A103537303635363733323635393034303709060101600101FF0A1236383431313338424E32343531303130393009060101010700FF060000033A09060101020700FF060000000009060101030700FF060000006809060101040700FF06000000B0090601011F0700FF06000000ED09060101330700FF060000005909060101470700FF060000004B09060101200700FF1200E809060101340700FF1200E909060101480700FF1200EC84467E"
hdlc_data_hex3 = (
    "7ea11d01000110b0aee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff060000024a09060100020700ff060000000009060100030700ff060000000009060100040700ff0600000109090601001f0700ff060000012d09060100330700ff060000056909060100470700ff06000005ef09060100200700ff060000090f09060100340700ff060000090709060100480700ff060000092009060000010000ff090c07e7081b0715202dffffc40009060100010800ff060107d8d009060100020800ff060000000009060100030800ff060000171e09060100040800ff06003bfaf201477e"
)
mqttPrefix = "misteln13power/KAIFA/"
mqttPower=mqttPrefix+"Power/Instant/"
mqttActive=mqttPower+"Active/"
mqttReactive=mqttPower+"Reactive/"
mqttCurrent=mqttPower.replace("Power","Current")
mqttVoltage=mqttPower.replace("Power","Voltage")
mqttEnergy=mqttPrefix+"Energy/"

prevPayload=b''
#clock,Reastatus = datetime_from_bytes(b"570656732659")
#clock, status = datetime_from_bytes(b"570656732659")
q = Queue()
crc=frames.CRCCCITT()
#tst=crc.calculate_for(bytes.fromhex(    "a11f010001f693aee6e700"))
def handle_msg(client, userdata, msg):
    if(msg.topic=="malarenergi/raw"):
        global prevPayload
        print(bytes.hex(msg.payload))
        payload=prevPayload+msg.payload
        msglen=int.from_bytes(payload[1:3],"big")-40960
        payloadLen=len(payload)
        lenDiff=msglen-payloadLen
        parse=False
        if(lenDiff==1 or lenDiff==-1 or lenDiff==-2):
            #Do not add flag, try to parse
            #7ea11d01000110b0aee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff060000009709060100020700ff060000000009060100030700ff060000000009060100040700ff06000000bf090601001f0700ff060000014f09060100330700ff06000001da09060100470700ff060000026f09060100200700ff06000008f009060100340700ff06000008fb09060100480700ff060000090309060000010000ff090c07e70906030f060fffffc40009060100010800ff06010a704709060100020800ff060000000009060100030800ff060000174d09060100040800ff06003cd47e
            parse=True
        elif(lenDiff>1):
            #Wait for next mqtt msg
            print("Uncomplete msg")
            skipLen=230
            if(lenDiff<skipLen):
                prevPayload=payload
            else:
                print("Skipping "+str(payload)+", too short")
        elif(lenDiff==0):
            #Add char and flag
            print("No lendiff")
            parse=True
        else:
            #7ea11d01000110b0aee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff06000000ec09060100020700ff060000000009060100030700ff060000000009060100040700ff06000000a1090601001f0700ff060000004609060100330700ff060000027d09060100470700ff06000002ac09060100200700ff060000091409060100340700ff060000090809060100480700ff060000091709060000010000ff090c07e7090805070f0fffffc40009060100010800ff06010ab04d09060100020800ff060000000009060100030800ff060000175309060100040800ff06003cf94c69
            print("Fel: diff={0} chars".format(lenDiff))
            parse=True

        
        if(parse):
            lastChar=payload[-1:]
            if(lastChar!=b'\x7e'):
                payload+=b'\x7e'
            payload=payload[3:]
            newLen=int(len(payload)+1)
            print("New length:{0}".format(newLen))
            byteMsgLen=(newLen+40960).to_bytes(2,"big")
            payload=b'~'+byteMsgLen+payload
            # Beräkna HCS-värdet och lägg till det i ramen
            fcsdata=payload[1:-3]
            fcs = crc.calculate_for(fcsdata)
            payload = payload[:-3] + fcs + payload[-1:]
            prevPayload=b''
            ui = frames.UnnumberedInformationFrame.from_bytes(payload)
            dn = xdlms.DataNotification.from_bytes(
                ui.payload[4:]
            )  # The first 3 bytes should be ignored. Even 4 for KAIFA

            result = parse_as_dlms_data(dn.body)
            q.put(result)
    else:
        #Get info and repost
        print(msg.topic)
        #print(type(msg.payload))
        jsonConfig: dict=json.loads(msg.payload)
        msgTime=jsonConfig['last_reset']
        year=msgTime[0:4]
        exclude=msg.topic.find("KAIFA-Meter-ID")
        if(year=="1970" and exclude==-1):
            #Change original message
            now=datetime.now()
            msgTime=now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            tst=msg.payload.replace(b'malarenergi/KAIFA', b'misteln13power/KAIFA').replace(b'1970-01-01T00:00:00+00:00',bytes(msgTime,"utf8"))
            msg.payload=tst
            q.put(msg)

#    time.sleep(1)
    
#    for b in msg.payload:
#        print(hex(b))

client=pahoclient.Client()
client.username_pw_set("appuser", "appuser")
if client.connect("192.168.222.20",1883,) != 0:
    print("Not able to connect to broker")
    sys.exit(1)
client.on_message=handle_msg

#pubret=client.publish("bas/active",12)
#client.disconnect()

client.subscribe("malarenergi/raw")
client.subscribe("homeassistant/sensor/#")

try:
    print("Ctrl+C to exit")
    client.loop_start()
    while(True):
        result=q.get()
        bMsg=isinstance(result, pahoclient.MQTTMessage)
        if(bMsg):
            #Publish msg
            pubret=client.publish(result.topic,result.payload,0,True)
        else:
            obisStr=""
            # rest is data
            for item in result:
                try:
                    if(len(obisStr)>0):
                        print(obisStr, end=", ")
                        if(obisStr=="0-0:1.0.0.255"):
                            clock,status=datetime_from_bytes(item)
                            strClock=clock.strftime("%Y/%m/%d %X")
                            print(strClock)
                            pubret=client.publish(mqttPrefix+"Time/value",strClock,1,True)
                        elif(obisStr=="1-0:1.7.0.255"):
                            pActivePower=item/1000
                            print("Active power+:\t{0} ".format(pActivePower))
                            pubret=client.publish(mqttActive+"Positive/value",pActivePower)
                            pubret=client.publish(mqttActive+"Positive/unit","kW")
                        elif(obisStr=="1-0:2.7.0.255"):
                            mActivePower=item/1000
                            print("Active power-:\t{0} ".format(mActivePower))
                            pubret=client.publish(mqttActive+"Negative/value",item)
                            pubret=client.publish(mqttActive+"Negative/unit","kW")
                        elif(obisStr=="1-0:3.7.0.255"):
                            pReactivePower=item/1000
                            print("ReActive power+:\t{0} ".format(pReactivePower))
                            pubret=client.publish(mqttReactive+"Positive/value",item)
                            pubret=client.publish(mqttReactive+"Positive/unit","kW")
                        elif(obisStr=="1-0:4.7.0.255"):
                            mReactivePower=item/1000
                            print("ReActive power-:\t{0} ".format(mReactivePower))
                            pubret=client.publish(mqttReactive+"Negative/value",item)
                            pubret=client.publish(mqttReactive+"Negative/unit","kW")
                        elif(obisStr=="1-0:31.7.0.255"):
                            current=item/1000
                            print("Ström L1: {0} A".format(current))
                            pubret=client.publish(mqttCurrent+"L1/value",current)
                            pubret=client.publish(mqttCurrent+"L1/unit","A")
                        elif(obisStr=="1-0:51.7.0.255"):
                            current=item/1000
                            print("Ström L2: {0} A".format(current))
                            pubret=client.publish(mqttCurrent+"L2/value",current)
                            pubret=client.publish(mqttCurrent+"L2/unit","A")
                        elif(obisStr=="1-0:71.7.0.255"):
                            current=item/1000
                            print("Ström L3: {0} A".format(current))
                            pubret=client.publish(mqttCurrent+"L3/value",current)
                            pubret=client.publish(mqttCurrent+"L3/unit","A")
                        elif(obisStr=="1-0:32.7.0.255"):
                            voltage=item/10
                            print("Spänning L1: {0} V".format(voltage))
                            pubret=client.publish(mqttVoltage+"L1/value",voltage)
                            pubret=client.publish(mqttVoltage+"L1/unit","V")
                        elif(obisStr=="1-0:52.7.0.255"):
                            voltage=item/10
                            print("Spänning L2: {0} V".format(voltage))
                            pubret=client.publish(mqttVoltage+"L2/value",voltage)
                            pubret=client.publish(mqttVoltage+"L2/unit","V")
                        elif(obisStr=="1-0:72.7.0.255"):
                            voltage=item/10
                            print("Spänning L3: {0} V".format(voltage))
                            pubret=client.publish(mqttVoltage+"L3/value",voltage)
                            pubret=client.publish(mqttVoltage+"L3/unit","V")
                        elif(obisStr=="1-0:1.8.0.255"):
                            totalkwh=item/1000
                            print("Total kWh: {0}".format(totalkwh))
                            pubret=client.publish(mqttEnergy+"Active/Positive/value",totalkwh)
                            pubret=client.publish(mqttEnergy+"Active/Positive/unit","kWh")
                        elif(obisStr=="1-0:2.8.0.255"):
                            totalkwh=item/1000
                            print("Total exported kWh: {0}".format(totalkwh))
                            pubret=client.publish(mqttEnergy+"Active/Negative/value",totalkwh)
                            pubret=client.publish(mqttEnergy+"Active/Negative/unit","kWh")
                        elif(obisStr=="1-0:3.8.0.255"):
                            totalkvarh=item/1000
                            print("Total reactive kVArh: {0}".format(totalkvarh))
                            pubret=client.publish(mqttEnergy+"Reactive/Positive/value",totalkvarh)
                            pubret=client.publish(mqttEnergy+"Reactive/Positive/unit","kVArh")
                        elif(obisStr=="1-0:4.8.0.255"):
                            totalkvarh=item/1000
                            print("Total exported reactive kVArh: {0}".format(totalkvarh))
                            pubret=client.publish(mqttEnergy+"Reactive/Negative/value",totalkvarh)
                            pubret=client.publish(mqttEnergy+"Reactive/Negative/unit","kVArh")
                        obisStr=''        
                    else:    
                        obis = Obis.from_bytes(item)
                        obisStr=obis.to_string()
                
                except Exception as e:
                    print(e)
                    obisErrPos=e.args[0].find("Not enough data to parse OBIS")
                    if(obisErrPos==0):
                        print(item)

except Exception as e:
    print(e)
finally:
    print("Disconnecting")
    client.disconnect


# pprint.pprint(result)
# First is date
# date_row = result.pop(0)
# clock_obis = Obis.from_bytes(date_row[0])
# clock, stats = datetime_from_bytes(date_row[1])
# print(f"Clock object: {clock_obis.to_string()}, datetime={clock}")

