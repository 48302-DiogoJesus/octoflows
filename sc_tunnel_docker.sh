# Docker API (for metrics). Runs on the client machine. Tunnel IP is from the external workers
ssh -L 127.0.0.1:2376:localhost:2375 diogojesus@10.15.0.14 -N