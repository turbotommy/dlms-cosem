import pprint
import paho.mqtt.client as paho
import sys
sys.path.append(r'C:\Dev\dlms-cosem')
from dlms_cosem.cosem import Obis
from dlms_cosem.hdlc import frames
from dlms_cosem.protocol import xdlms
from dlms_cosem.time import datetime_from_bytes
from dlms_cosem.utils import parse_as_dlms_data

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

#clock, status = datetime_from_bytes(b"570656732659")

#crc=frames.CRCCCITT()
#tst=crc.calculate_for(bytes.fromhex(    "a11f010001f693aee6e700"))
def handle_msg(client, userdata, msg):
    print(bytes.hex(msg.payload))
    bArray=bytearray(msg.payload)
    bArray+=b'\x7e'
    ui = frames.UnnumberedInformationFrame.from_bytes(bArray)
    dn = xdlms.DataNotification.from_bytes(
        ui.payload[4:]
    )  # The first 3 bytes should be ignored. Even 4 for KAIFA

    result = parse_as_dlms_data(dn.body)

    obisStr=""
    # rest is data
    for item in result:
        try:
            if(len(obisStr)>0):
                print(obisStr)
                if(obisStr=="0-0:1.0.0.255"):
                    clock,status=datetime_from_bytes(item)
                    print(clock.isoformat)
                elif(obisStr=="1-0:1.7.0.255"):
                    print("Active power+")
                elif(obisStr=="1-0:2.7.0.255"):
                    print("Active power-")
                elif(obisStr=="1-0:3.7.0.255"):
                    print("ReActive power+")
                elif(obisStr=="1-0:4.7.0.255"):
                    print("ReActive power-")
                elif(obisStr=="1-0:31.7.0.255"):
                    print("Ström L1: {0}".format(item/100))
                elif(obisStr=="1-0:51.7.0.255"):
                    print("Ström L2: {0}".format(item/100))
                elif(obisStr=="1-0:71.7.0.255"):
                    print("Ström L3: {0}".format(item/100))
                elif(obisStr=="1-0:32.7.0.255"):
                    print("Spänning L1: {0}".format(item/10))
                elif(obisStr=="1-0:52.7.0.255"):
                    print("Spänning L2: {0}".format(item/10))
                elif(obisStr=="1-0:72.7.0.255"):
                    print("Spänning L3: {0}".format(item/10))
                elif(obisStr=="1-0:1.8.0.255"):
                    print("Total meter: {0}".format(item))
                    
                
            obisStr=''        
            obis = Obis.from_bytes(item)
            obisStr=obis.to_string()
        except Exception:
            print(item)

    clock, status = datetime_from_bytes(b"570656732659")
    print(clock)
#    for b in msg.payload:
#        print(hex(b))

client=paho.Client()
client.on_message=handle_msg
client.username_pw_set("appuser", "appuser")
if client.connect("misteln13",1883,) != 0:
    print("Not able to connect to broker")
    sys.exit(1)

#client.disconnect()

client.subscribe("malarenergi/raw")

try:
    print("Ctrl+C to exit")
    client.loop_forever()
except Exception:
    print(Exception)
finally:
    print("Disconnecting")
    client.disconnect


# pprint.pprint(result)
# First is date
# date_row = result.pop(0)
# clock_obis = Obis.from_bytes(date_row[0])
# clock, stats = datetime_from_bytes(date_row[1])
# print(f"Clock object: {clock_obis.to_string()}, datetime={clock}")

