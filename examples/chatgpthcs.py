def encode_hdlc_frame(data):
    # Lägg till flaggor i början och slutet av ramen
    frame = b'\x7e' + data.replace(b'\x7e', b'\x7d\x5e').replace(b'\x7d', b'\x7d\x5d') + b'\x7e'

    # Beräkna HCS-värdet och lägg till det i ramen
    hcs = crc16(frame[1:-3])
    frame = frame[:-3] + hcs.to_bytes(2, byteorder='big') + frame[-1:]

    return frame

def decode_hdlc_frame(frame):
    # Ta bort flaggor från början och slutet av ramen
    data = frame[1:-1]

    # Avkoda datafältet och ersätt escape-sekvenser
    data = data.replace(b'\x7d\x5e', b'\x7e').replace(b'\x7d\x5d', b'\x7d')

    # Kontrollera HCS-värdet
    expected_hcs = crc16(frame[1:-3])
    actual_hcs = int.from_bytes(frame[-3:-1], byteorder='big')
    if expected_hcs != actual_hcs:
        raise ValueError('Invalid HCS value')

    return data

def crc16(data):
    # CRC-16-CCITT-algoritmen
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
    return crc

# Användningsexempel
data = b'Hello World'
frame = encode_hdlc_frame(data)
print(frame.hex())
decoded_data = decode_hdlc_frame(frame)
print(decoded_data)
