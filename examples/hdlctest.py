import sys
import paho.mqtt.client as paho
import sys
sys.path.append(r'C:\Dev\dlms-cosem')
from dlms_cosem.protocol import xdlms
from dlms_cosem.hdlc import frames
from dlms_cosem.utils import parse_as_dlms_data
from dlms_cosem.cosem import Obis
from dlms_cosem.time import datetime_from_bytes

# 3-phase
hdlc_data_hex = (
    "7ea11d01000110b0aee6e7000f40000000000224"
    "09060100000281ff09074b464d5f303031"
    "09060000600100ff091037333430313537303330373332303434"
    "09060000600107ff09074d413330344834
    "09060100010700ff060000022d
    "09060100020700ff0600000000
    "09060100030700ff0600000000"
    "09060100040700ff0600000172"
    "090601001f0700ff0600000053"
    "09060100330700ff0600000484"
    "09060100470700ff060000074b"
    "09060100200700ff060000091f"
    "09060100340700ff0600000914"
    "09060100480700ff060000091d"
    "09060000010000ff090c07e7090a0711302dffffc400"
    "09060100010800ff06010c467e
)
hdlc_data_hex3 = (
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
# 11d -> framesize
# "7ea11d01000110b0aee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff06000000a009060100020700ff060000000009060100030700ff060000000009060100040700ff0600000118090601001f0700ff06000001a909060100330700ff060000022c09060100470700ff060000028f09060100200700ff060000092209060100340700ff060000092609060100480700ff060000092d09060000010000ff090c07e708140706251effffc40009060100010800ff06010567e509060100020800ff060000000009060100030800ff060000170309060100040800ff06003b40a3898a7e"
# "7ea11d01000101b8afaee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff060000009d09060100020700ff060000000009060100030700ff060000000009060100040700ff0600000110090601001f0700ff060000018109060100330700ff060000023a09060100470700ff060000029309060100200700ff060000091d09060100340700ff060000091b09060100480700ff060000093009060000010000ff090c07e7081407083a00ffffc40009060100010800ff06010569b309060100020800ff060000000009060100030800ff060000170309060100040800ff06003b434052727e"
#hex2=Ojämnt från mätare
hdlc_data_hex2 = (
    #0
    "7ea11d01000110b0aee6e7000f4000000000022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff06000002cd09060100020700ff060000000009060100030700ff060000000009060100040700ff060000011c090601001f0700ff060000080909060100330700ff060000049509060100470700ff06000002a909060100200700ff060000090b09060100340700ff060000090a09060100480700ff060000091909060000010000ff090c07e7090a07171600ffffc40009060100010800ff06010c586909060100020800ff060000000009060100030800ff060000175d09060100040800ff06003d367e"
)

hdlc_data_hex_kode = (
    "7EA0E22B2113239AE6E7000F000000000C07E306120214"
    "2F32FF80008002190A0E4B616D73747275705F56303030"
    "3109060101000005FF0A10323230303536373232333139"
    "3737313409060101600101FF0A1236383431313331424E"
    "32343331303130343009060101010700FF06000006A709"
    "060101020700FF060000000009060101030700FF060000"
    "000009060101040700FF06000001E0090601011F0700FF"
    "060000008809060101330700FF06000002360906010147"
    "0700FF060000006D09060101200700FF1200EB09060101"
    "340700FF1200EB09060101480700FF1200EB83777E"
)
crc=frames.CRCCCITT()
payload=bytes.fromhex(hdlc_data_hex2)
payload+=b'\x10\x7e'
skipflags=payload[3:len(payload)-3]
tst=crc.calculate_for(skipflags)
msglen=int.from_bytes(payload[1:3],"big")-40960

lenDiff=msglen-len(payload)

ui = frames.UnnumberedInformationFrame.from_bytes(payload)
dn = xdlms.DataNotification.from_bytes(
    ui.payload[4:]
)  # The first 3 or 4 bytes should be ignored.
result = parse_as_dlms_data(dn.body)

obisStr=""
# rest is data
for item in result:
    try:
        if(len(obisStr)>0):
            print(obisStr, end="   \t")
            if(obisStr=="0-0:96.1.0.255"):
                meterId=item.decode()
                print("Meter ID:\t{0}".format(meterId))
            elif(obisStr=="0-0:1.0.0.255"):
                clock,status=datetime_from_bytes(item)
                strClock=clock.strftime("%Y-%m-%d %X")
                print(strClock)
            elif(obisStr=="1-0:0.2.129.255"):
                meterName=item
                print("Meter name:\t{0}".format(meterName.decode()))
            elif(obisStr=="1-0:1.7.0.255"):
                pActivePower=item
                print("Active power+:\t{0} ".format(pActivePower))
            elif(obisStr=="1-0:2.7.0.255"):
                mActivePower=item
                print("Active power-:\t{0} ".format(mActivePower))
            elif(obisStr=="1-0:3.7.0.255"):
                print("ReActive power+")
            elif(obisStr=="1-0:4.7.0.255"):
                print("ReActive power-")
            elif(obisStr=="1-0:31.7.0.255"):
                current=item/1000
                print("Ström L1: {0} A".format(current))
            elif(obisStr=="1-0:51.7.0.255"):
                current=item/1000
                print("Ström L2: {0} A".format(current))
            elif(obisStr=="1-0:71.7.0.255"):
                current=item/1000
                print("Ström L3: {0} A".format(current))
            elif(obisStr=="1-0:32.7.0.255"):
                voltage=item/10
                print("Spänning L1: {0} V".format(voltage))
            elif(obisStr=="1-0:52.7.0.255"):
                voltage=item/10
                print("Spänning L2: {0} V".format(voltage))
            elif(obisStr=="1-0:72.7.0.255"):
                voltage=item/10
                print("Spänning L3: {0} V".format(voltage))
            elif(obisStr=="1-0:1.8.0.255"):
                totalkwh=item/1000
                print("Total kWh: {0}".format(totalkwh))
            elif(obisStr=="1-0:2.8.0.255"):
                totalkwh=item/1000
                print("Total exported kWh: {0}".format(totalkwh))
            elif(obisStr=="1-0:3.8.0.255"):
                totalkvarh=item/1000
                print("Total reactive kVArh: {0}".format(totalkvarh))
            elif(obisStr=="1-0:4.8.0.255"):
                totalkvarh=item/1000
                print("Total exported reactive kVArh: {0}".format(totalkvarh))
            obisStr=''        
        else:    
            obis = Obis.from_bytes(item)
            obisStr=obis.to_string()
    
    except Exception as e:
        print(e)
        obisErrPos=e.args[0].find("Not enough data to parse OBIS")
        if(obisErrPos==0):
            print(item)

sys.exit(0)

def handle_msg(client, userdata, msg):
    
    print(bytes.hex(msg.payload))
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

in_file = open(r"C:\Users\TommyEkh\OneDrive - Coop Sverige AB\Eget\Energi\energytest.org", "rb") # opening for [r]eading as [b]inary
data = in_file.read() # if you only wanted to read 512 bytes, do .read(512)
 
# Look for 09
pos09=data.find(9) #Look for control char
pos09+=1
blockLen=data[pos09] #Blocksize
# Get block
block=data[pos09+1:blockLen+pos09+1]
obisTest=Obis.from_bytes(block)
print(obisTest.to_string())
in_file.close()

#test=frames.InformationFrame.from_bytes(bytes.fromhex(hdlc_data_hex_kode))
crc=frames.CRCCCITT()
tst=crc.calculate_for(bytes.fromhex(    "a121010001b8afaee6e700"
    "0f4000000000011b022409060100000281ff09074b464d5f30303109060000600100ff09103733343031353730333037333230343409060000600107ff09074d41333034483409060100010700ff06000000a009060100020700ff060000000009060100030700ff060000000009060100040700ff0600000118090601001f0700ff06000001a909060100330700ff060000022c09060100470700ff060000028f09060100200700ff060000092209060100340700ff060000092609060100480700ff060000092d09060000010000ff090c07e708140706251effffc40009060100010800ff06010567e509060100020800ff060000000009060100030800ff060000170309060100040800ff06003b40a3"
))

