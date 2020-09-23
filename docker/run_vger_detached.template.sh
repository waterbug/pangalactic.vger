docker run -d --restart unless-stopped \
  -v ~/vger_home:/vger_home \
  -v [local crossbar home]:/vger_home/crossbar \
  --network=host vger:x.x.x

