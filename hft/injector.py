import socket
import struct
import time

UDP_IP = "127.0.0.1"
UDP_PORT = 8080

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Blasting 1,000,000 UDP packets to {UDP_IP}:{UDP_PORT}...")

# '<' means little-endian, no alignment padding
# 'c' = 1 byte char
# 'Q' = 8 byte unsigned long long (u64)
# 'I' = 4 byte unsigned int (u32)
# Total = 21 bytes, exactly matching our Rust zero-copy parser.

start_time = time.time()

for i in range(1_000_000):
    packet = struct.pack('<cQQI', b'B', 15000, i, 10)
    sock.sendto(packet, (UDP_IP, UDP_PORT))

end_time = time.time()
print(f"Python finished sending 1,000,000 packets in {end_time - start_time:.2f} seconds.")