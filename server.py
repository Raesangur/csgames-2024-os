# Pascal-Emmanuel Lachance
# Julien Champoux
# Université de Sherbrooke

# Désolé pour le dictionnaire...

# Implemented
# single file upload, multiple file uploads, multiple crawlers,
# send correct responses (?), handle compressed mode (runlength), handle out of order

import socket
from pathlib import Path

HOST = "localhost"
PORT = 7331

def run_length_decode(data: bytearray) -> bytearray:
    current_pos = 0
    true_data = bytearray()
    while current_pos < len(data) - 1:
        char = ord(data[current_pos:].decode()[0])
        current_pos += len(chr(char).encode('utf-8'))
        rl = data[current_pos]
        current_pos += 1
        true_data += rl * int.to_bytes(char)
    return true_data

def decode_header(data: bytearray) -> tuple[int, str, bytearray]:
    data_length = int.from_bytes(data[2:4], byteorder='big')
    crawler_id = int.from_bytes(data[4:6], byteorder='big')
    command = data[6:10].decode()
    payload = data[10:10+data_length]
    return crawler_id, command, payload

def assemble_transfer(dl_id: int, transfers: dict, s: socket.socket) -> bool:
    # Assemble transfer
    length = max(transfers[dl_id].keys())
    full_data = bytearray()

    #missed_transfers = []
    for i in range(transfers[dl_id][-2], length + 1):
        if i in transfers[dl_id].keys():
            full_data += transfers[dl_id][i]
        #else:
            #missed_transfers += i

    #if len(missed_transfers) > 0:
    #    payload = b"LOSS" + len(missed_transfers).to_bytes(byteorder="big", length=2)
    #    payload += b''.join([seq_id.to_bytes(byteorder="big", length=2) for seq_id in missed_transfers])
    #    s.sendto(payload, addr)
    #    return False

    if full_data[-1] == 0:
        full_data = full_data[:-1]

    path = Path('dumps/' + transfers[dl_id][-1])
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(full_data)
    print(f'Dumped `{path}` with {len(full_data)} bytes.')

    return True


if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind((HOST, PORT))
        transfers = {}
        next_upld_id = 0

        while True:
            data, addr = s.recvfrom(2048)
            data = run_length_decode(data[1:])
            crawler_id, command, payload = decode_header(data)
            #print("ID: " + crawler_id)
            #print("CMD: " + command)
            #print("PYLD: " + payload.decode())

            match command:
                case 'UPLD':
                    s.sendto(b"UPLOADING"+next_upld_id.to_bytes(length=2, byteorder='big'), addr)
                    transfers[next_upld_id] = {-1: payload.decode().replace('\x00', '')}
                    next_upld_id += 1

                case 'DATA':
                    dl_id = int.from_bytes(payload[0:2], byteorder='big')
                    seq_id = int.from_bytes(payload[2:4], byteorder='big')
                    block_data = payload[4:]
                    if -2 not in transfers[dl_id].keys():
                        transfers[dl_id][-2] = seq_id
                    transfers[dl_id][seq_id] = block_data

                    if -3 in transfers[dl_id]:
                        assemble_transfer(dl_id, transfers, s)

                    if len(block_data) == 0:
                        transfers[dl_id][-3] = "DONE"
                        assemble_transfer(dl_id, transfers, s)
                    else:
                        s.sendto(b"UPLOADING"+next_upld_id.to_bytes(length=2, byteorder='big'), addr)
                        





