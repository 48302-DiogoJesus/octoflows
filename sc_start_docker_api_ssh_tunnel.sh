# Docker API (for metrics). Runs on the client machine. Tunnel IP is from the external workers
ssh -L 2376:127.0.0.1:2375 diogojesus@proteina04 -N